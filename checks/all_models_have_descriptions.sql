-- Return enabled models in this project that have no meaningful description.
select
    unique_id,
    name
from {{ info_schema('models') }}
where enabled
    and package_name = 'foundational_project'
    and trim(coalesce(description, '')) = ''

