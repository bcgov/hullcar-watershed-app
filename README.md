# Hullcar Watershed App
This project provides a public-facing ArcGIS Online application that visualizes 
environmental monitoring data for the Clcahl/Hullcar Aquifer, a vulnerable 
groundwater source located in the Township of Spallumcheen, British Columbia. 

The aquifer supplies drinking water to residents via both private wells and the 
Steele Springs Waterworks District. In response to long-standing water quality 
concerns (particularly elevated nitrate levels), this project supports greater 
transparency and accessibility of water quality data.

It brings together current and historic sampling results from the BC 
Environmental Monitoring System (EMS), automates data processing through a 
GitHub-based pipeline, and regularly publishes updated results to an ArcGIS
Online web map.



# Import Links
[AGOL Group](https://governmentofbc.maps.arcgis.com/home/group.html?id=e8f58ee68fc944f3a56bd0ba5667613b) (Used for storing hosted feature layers and web app components such as maps and dashboards). 

[![img](https://img.shields.io/badge/Lifecycle-Experimental-339999)](https://github.com/bcgov/repomountie/blob/master/doc/lifecycle-badges.md)

[Hullcar Aquifer Public Information  Page](https://www2.gov.bc.ca/gov/content/environment/air-land-water/site-permitting-compliance/hullcar-aquifer): The current information page available to the public. The ArcGIS Online application will supplement the information
here.

## Contributing
We encourage contributions. Please see our [Contributing document](<CONTRIBUTING.md>). BC Government employees should also ensure they review [BC Open Source Development Employee Guide](https://github.com/bcgov/BC-Policy-Framework-For-GitHub/blob/master/BC-Open-Source-Development-Employee-Guide/README.md)

## Getting Help or Reporting an Issue
To report bugs, issues, or feature requests please file an issue.

## Github Secrets
This project uses Github Actions to automate data updates. The script 
"scripts/upload_ems_data_to_ago.py" relies on the following secrets which 
can be configured in the repository settings under **Settings > Secrets and 
Variables > Actions**

#### Required Secrets
| Secret Name            | Description                                                                 |
|------------------------|-----------------------------------------------------------------------------|
| `CKAN_API_URL`         | URL for the CKAN instance used for the BC Data Catalogue.                   |
| `GSS_ES_AGO_USERNAME`  | ArcGIS Online username for uploading or managing feature layers.            |
| `GSS_ES_AGO_PASSWORD`  | ArcGIS Online password associated with the username.                        |
| `HULLCAR_GROUP_ID`     | AGOL Group ID used to organize and share the web app and data layers.       |
| `HULLCAR_ITEM_ID`      | ArcGIS Online Item ID for the Hullcar Aquifer hosted feature layer.    |
| `MAPHUB_URL`           | URL endpoint for accessing MapHub and related services.                      |


## License
    Copyright 2019 BC Provincial Government

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
