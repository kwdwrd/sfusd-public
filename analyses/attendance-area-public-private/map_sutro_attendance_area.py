#!/usr/bin/env python3
"""
Map Sutro Elementary's attendance area with overlapping census tracts
"""

import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as cx
import pandas as pd
import requests
import sys

def load_acs_data(census_api_key):
    """Load ACS enrollment data for SF census tracts (ages 5-9)"""
    base_url = "https://api.census.gov/data/2023/acs/acs5"

    variables = [
        'NAME',
        # Male public school
        'B14003_005E',  # Male 5-9 public
        # Male private school
        'B14003_014E',  # Male 5-9 private
        # Male not enrolled
        'B14003_023E',  # Male 5-9 not enrolled
        # Female public school
        'B14003_033E',  # Female 5-9 public
        # Female private school
        'B14003_042E',  # Female 5-9 private
        # Female not enrolled
        'B14003_051E',  # Female 5-9 not enrolled
    ]

    params = {
        'get': ','.join(variables),
        'for': 'tract:*',
        'in': 'state:06 county:075',
        'key': census_api_key
    }

    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        print(f"API Error: {response.status_code} - {response.text}")
        sys.exit(1)

    acs_data = pd.DataFrame(response.json()[1:], columns=response.json()[0])

    # Convert to numeric
    numeric_cols = [col for col in acs_data.columns if col not in ['NAME', 'state', 'county', 'tract']]
    for col in numeric_cols:
        acs_data[col] = pd.to_numeric(acs_data[col], errors='coerce')

    # Create GEOID
    acs_data['tract_id'] = acs_data['state'] + acs_data['county'] + acs_data['tract']

    # Calculate totals (ages 5-9 only)
    acs_data['public_total'] = (
        acs_data['B14003_005E'] +  # Male 5-9 public
        acs_data['B14003_033E']    # Female 5-9 public
    )

    acs_data['private_total'] = (
        acs_data['B14003_014E'] +  # Male 5-9 private
        acs_data['B14003_042E']    # Female 5-9 private
    )

    acs_data['not_enrolled_total'] = (
        acs_data['B14003_023E'] +  # Male 5-9 not enrolled
        acs_data['B14003_051E']    # Female 5-9 not enrolled
    )

    acs_data['total_children'] = (
        acs_data['public_total'] + acs_data['private_total'] +
        acs_data['not_enrolled_total']
    )

    # Calculate percentages
    acs_data['pct_public'] = (acs_data['public_total'] / acs_data['total_children'] * 100).round(0)
    acs_data['pct_private'] = (acs_data['private_total'] / acs_data['total_children'] * 100).round(0)

    return acs_data

def main():
    # Get Census API key
    census_api_key = input("Enter your Census API key (get one at https://api.census.gov/data/key_signup.html): ").strip()
    if not census_api_key:
        print("ERROR: Census API key is required")
        sys.exit(1)

    print("Loading SFUSD attendance areas...")
    sfusd_url = "https://data.sfgov.org/resource/e6tr-sxwg.geojson"
    attendance_areas = gpd.read_file(sfusd_url)
    attendance_areas = attendance_areas[attendance_areas['e_aa_name'].notna()].copy()

    if attendance_areas.crs is None:
        attendance_areas.set_crs(epsg=4326, inplace=True)

    # Get Sutro attendance area
    sutro = attendance_areas[attendance_areas['e_aa_name'] == 'SUTRO'].copy()
    if len(sutro) == 0:
        print("ERROR: Could not find SUTRO attendance area")
        return

    print(f"Found Sutro Elementary attendance area")

    print("Loading San Francisco census tracts...")
    tracts_url = "https://www2.census.gov/geo/tiger/TIGER2023/TRACT/tl_2023_06_tract.zip"
    tracts = gpd.read_file(tracts_url)
    sf_tracts = tracts[tracts['COUNTYFP'] == '075'].copy()
    sf_tracts = sf_tracts.to_crs(attendance_areas.crs)
    sf_tracts['tract_id'] = sf_tracts['GEOID']

    print(f"Loaded {len(sf_tracts)} SF census tracts")

    # Load ACS enrollment data
    print("Fetching ACS enrollment data...")
    acs_data = load_acs_data(census_api_key)
    print(f"Loaded enrollment data for {len(acs_data)} tracts")

    # Merge ACS data with tracts
    sf_tracts = sf_tracts.merge(
        acs_data[['tract_id', 'public_total', 'private_total', 'total_children', 'pct_public', 'pct_private']],
        on='tract_id',
        how='left'
    )

    # Find tracts that intersect with Sutro
    intersecting_tracts = sf_tracts[sf_tracts.intersects(sutro.union_all())]
    print(f"Found {len(intersecting_tracts)} census tracts intersecting Sutro")

    # Convert to Web Mercator for contextily basemap
    sutro_wm = sutro.to_crs(epsg=3857)
    sf_tracts_wm = sf_tracts.to_crs(epsg=3857)
    intersecting_tracts_wm = intersecting_tracts.to_crs(epsg=3857)

    # Create the map
    fig, ax = plt.subplots(1, 1, figsize=(12, 10))

    # Get bounds for clipping (with some padding)
    bounds = sutro_wm.total_bounds
    padding = 500  # meters
    xlim = (bounds[0] - padding, bounds[2] + padding)
    ylim = (bounds[1] - padding, bounds[3] + padding)

    # Plot all SF tracts in light gray for context
    sf_tracts_wm.plot(ax=ax, color='#f0f0f0', edgecolor='#cccccc', linewidth=0.5, alpha=0.5)

    # Plot intersecting tracts
    intersecting_tracts_wm.plot(ax=ax, color='#cce5ff', edgecolor='#0066cc', linewidth=1.5, alpha=0.6)

    # Plot Sutro attendance area
    sutro_wm.plot(ax=ax, color='none', edgecolor='#cc0000', linewidth=3, linestyle='-')

    # Set map bounds before adding basemap
    ax.set_xlim(xlim)
    ax.set_ylim(ylim)

    # Add street basemap
    print("Adding street basemap...")
    cx.add_basemap(ax, source=cx.providers.CartoDB.Positron, zoom=15)

    # Add tract labels with enrollment data
    for idx, row in intersecting_tracts_wm.iterrows():
        centroid = row.geometry.centroid

        # For tracts with centroids outside view, use the intersection centroid instead
        if xlim[0] <= centroid.x <= xlim[1] and ylim[0] <= centroid.y <= ylim[1]:
            label_x, label_y = centroid.x, centroid.y
        else:
            # Clip the tract to the view bounds and use that centroid
            from shapely.geometry import box
            view_box = box(xlim[0], ylim[0], xlim[1], ylim[1])
            clipped = row.geometry.intersection(view_box)
            if clipped.is_empty:
                continue
            label_x, label_y = clipped.centroid.x, clipped.centroid.y

        # Build label with tract number and enrollment stats
        total = row.get('total_children', 0)
        pct_pub = row.get('pct_public', 0)
        pct_priv = row.get('pct_private', 0)

        if pd.notna(total) and total > 0:
            label = f"Tract {row['TRACTCE']}\nn={int(total)}\nPub: {int(pct_pub)}% Priv: {int(pct_priv)}%"
        else:
            label = f"Tract {row['TRACTCE']}\n(no data)"

        ax.annotate(
            label,
            xy=(label_x, label_y),
            ha='center', va='center',
                fontsize=8,
                fontweight='bold',
                color='#003366',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='#cccccc', alpha=0.85)
            )

    # Styling
    ax.set_title('Sutro Elementary Attendance Area\nwith Intersecting Census Tracts (Ages 5-9 Enrollment)', fontsize=14, fontweight='bold')
    ax.set_axis_off()

    # Add legend
    from matplotlib.patches import Patch
    from matplotlib.lines import Line2D
    legend_elements = [
        Patch(facecolor='#cce5ff', edgecolor='#0066cc', linewidth=1.5, alpha=0.6, label='Intersecting Census Tracts'),
        Line2D([0], [0], color='#cc0000', linewidth=3, label='Sutro Attendance Area'),
    ]
    ax.legend(handles=legend_elements, loc='upper right', framealpha=0.9)

    # Save the map
    output_file = 'sutro_attendance_area_map.png'
    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"\n✓ Map saved to: {output_file}")

    plt.show()

if __name__ == "__main__":
    main()
