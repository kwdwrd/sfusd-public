#!/usr/bin/env python3
"""
SFUSD Enrollment Analysis by Elementary Attendance Area
Analyzes public vs private school enrollment using ACS data and SFUSD boundaries
"""

import geopandas as gpd
import pandas as pd
import requests
import sys

def main():
    """Main analysis function"""
    
    print("="*80)
    print("SFUSD ENROLLMENT ANALYSIS BY ELEMENTARY ATTENDANCE AREA")
    print("="*80)
    
    # Configuration
    census_api_key = input("\nEnter your Census API key (get one at https://api.census.gov/data/key_signup.html): ").strip()
    if not census_api_key:
        print("ERROR: Census API key is required")
        sys.exit(1)
    
    # Step 1: Load SFUSD Attendance Areas
    print("\n[1/6] Loading SFUSD attendance areas...")
    try:
        sfusd_url = "https://data.sfgov.org/resource/e6tr-sxwg.geojson"
        attendance_areas = gpd.read_file(sfusd_url)
        attendance_areas = attendance_areas[attendance_areas['e_aa_name'].notna()].copy()
        attendance_areas = attendance_areas[['e_aa_name', 'e_aa_schno', 'geometry']]
        
        if attendance_areas.crs is None:
            attendance_areas.set_crs(epsg=4326, inplace=True)
        
        print(f"   ✓ Loaded {len(attendance_areas)} elementary attendance areas")
    except Exception as e:
        print(f"   ✗ Error loading attendance areas: {e}")
        sys.exit(1)
    
    # Step 2: Load Census Tracts
    print("\n[2/6] Loading San Francisco census tracts...")
    try:
        tracts_url = "https://www2.census.gov/geo/tiger/TIGER2023/TRACT/tl_2023_06_tract.zip"
        tracts = gpd.read_file(tracts_url)
        sf_tracts = tracts[tracts['COUNTYFP'] == '075'].copy()
        sf_tracts['tract_id'] = sf_tracts['GEOID']
        sf_tracts = sf_tracts.to_crs(attendance_areas.crs)
        
        print(f"   ✓ Loaded {len(sf_tracts)} San Francisco census tracts")
    except Exception as e:
        print(f"   ✗ Error loading census tracts: {e}")
        sys.exit(1)
    
    # Step 3: Get ACS enrollment data
    print("\n[3/6] Fetching ACS enrollment data from Census API...")
    try:
        base_url = "https://api.census.gov/data/2023/acs/acs5"
        
        # Default to ages 5-9 only (elementary school age)
        # Ages 10-14 can be included but attendance patterns differ for middle/high school
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
            print(f"   ✗ API Error: {response.status_code} - {response.text}")
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
        
        print(f"   ✓ Retrieved enrollment data for {len(acs_data)} census tracts")
    except Exception as e:
        print(f"   ✗ Error fetching ACS data: {e}")
        sys.exit(1)
    
    # Step 4: Join ACS data to census tracts
    print("\n[4/6] Joining enrollment data to census tracts...")
    sf_tracts = sf_tracts.merge(
        acs_data[['tract_id', 'public_total', 'private_total', 
                  'not_enrolled_total', 'total_children']], 
        on='tract_id', 
        how='left'
    )
    print(f"   ✓ Joined data to {len(sf_tracts)} tracts")
    
    # Step 5: Spatial join with area-weighted allocation
    print("\n[5/6] Performing spatial join (area-weighted allocation)...")
    try:
        # Calculate intersections
        intersections = gpd.overlay(sf_tracts, attendance_areas, how='intersection')
        intersections['intersection_area'] = intersections.geometry.area
        
        # Calculate proportions
        tract_areas = sf_tracts.set_index('tract_id').geometry.area
        intersections['tract_area'] = intersections['tract_id'].map(tract_areas)
        intersections['proportion'] = intersections['intersection_area'] / intersections['tract_area']
        
        # Allocate enrollment proportionally
        for col in ['public_total', 'private_total', 'not_enrolled_total', 'total_children']:
            intersections[f'{col}_allocated'] = intersections[col] * intersections['proportion']
        
        print(f"   ✓ Created {len(intersections)} tract-attendance area intersections")
    except Exception as e:
        print(f"   ✗ Error in spatial join: {e}")
        sys.exit(1)
    
    # Step 6: Aggregate by attendance area
    print("\n[6/6] Aggregating enrollment by attendance area...")
    results = intersections.groupby('e_aa_name').agg({
        'public_total_allocated': 'sum',
        'private_total_allocated': 'sum',
        'not_enrolled_total_allocated': 'sum',
        'total_children_allocated': 'sum'
    }).round(0)
    
    # Rename columns
    results.columns = ['Public School', 'Private School', 'Not Enrolled', 'Total Children']
    
    # Calculate percentages
    results['% Public'] = (results['Public School'] / results['Total Children'] * 100).round(1)
    results['% Private'] = (results['Private School'] / results['Total Children'] * 100).round(1)
    results['% Not Enrolled'] = (results['Not Enrolled'] / results['Total Children'] * 100).round(1)
    
    # Sort by attendance area name
    results = results.sort_index()
    
    # Display results
    print("\n" + "="*100)
    print("RESULTS: ELEMENTARY SCHOOL ENROLLMENT BY SFUSD ATTENDANCE AREA")
    print("="*100)
    print(results.to_string())
    
    # Summary statistics
    print("\n" + "="*100)
    print("SUMMARY STATISTICS")
    print("="*100)
    total = results['Total Children'].sum()
    public = results['Public School'].sum()
    private = results['Private School'].sum()
    not_enrolled = results['Not Enrolled'].sum()
    
    print(f"Total Elementary Attendance Areas: {len(results)}")
    print(f"Total Children (ages 5-9): {total:,.0f}")
    print(f"  - Public School:     {public:,.0f}  ({public/total*100:.1f}%)")
    print(f"  - Private School:    {private:,.0f}  ({private/total*100:.1f}%)")
    print(f"  - Not Enrolled:      {not_enrolled:,.0f}  ({not_enrolled/total*100:.1f}%)")
    
    print(f"\nHighest Public School %:")
    top_public = results.nlargest(5, '% Public')[['Total Children', 'Public School', '% Public']]
    print(top_public.to_string())
    
    print(f"\nHighest Private School %:")
    top_private = results.nlargest(5, '% Private')[['Total Children', 'Private School', '% Private']]
    print(top_private.to_string())
    
    # Save to CSV
    output_file = 'sfusd_enrollment_by_attendance_area.csv'
    results.to_csv(output_file)
    print(f"\n✓ Results saved to: {output_file}")
    
    # Save detailed data with all intersections
    detailed_file = 'sfusd_enrollment_detailed_intersections.csv'
    intersections.drop(columns=['geometry']).to_csv(detailed_file, index=False)
    print(f"✓ Detailed intersections saved to: {detailed_file}")
    
    print("\n" + "="*100)
    print("Analysis complete!")
    print("="*100)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nAnalysis interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
