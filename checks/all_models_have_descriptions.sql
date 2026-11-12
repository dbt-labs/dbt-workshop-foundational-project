select unique_id
from {{ info_schema('models') }}
where enabled
  and package_name = (
      select project_name
      from {{ info_schema('project') }}
  )
  and trim(coalesce(description, '')) = ''
