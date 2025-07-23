{% snapshot dim_provider_snapshot %}
    {{
        config(
            target_database='HEALTHCARE_DB',
            target_schema='SNAPSHOTS',
            unique_key=['provider_id'],
            strategy='check',
            check_cols=[
                'provider_name', 'provider_address', 'city_town', 'state', 'zip_code', 'telephone_number', 
                'county_name', 'cms_region', 'provider_ssa_county_code', 'ownership_type', 'provider_type', 
                'legal_business_name', 'provider_resides_in_hospital', 'continuing_care_retirement_community', 
                'with_a_resident_and_family_council', 'automatic_sprinkler_systems_in_all_required_areas', 
                'provider_changed_ownership_in_last_12_months', 'latitude', 'longitude'
            ]
        )
    }}

    select * from {{ ref('dim_provider') }}

{% endsnapshot %}
