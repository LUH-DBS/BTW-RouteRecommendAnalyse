# Recommending Alternative Cycling Routes via Predicted Usage Patterns
Dakai Men, Jannis Becktepe, Mahdi Esmailoghli, David Bermbach, Ziawasch Abedjan

---

## General

1. This repository is the implementation of Data Science Challenge of the 30th BTW conference
2. All database and path-related information is not included and should be adapted manually
3. Each file in the directory `luhbike` serves as a stand-alone script for a specific module and can be executed individually


## Components

* luhbike/alternative_route_loader.py: load alternative routes from Open Routing Service
* luhbike/classification.py: classification of the selected route and alternative route
* luhbike/env_loader.py: loading environmental (weather) data
* luhbike/feature.py: generating the final feature vectors
* luhbike/find_alternative_rides.py: finding an alternative route corresponding to a selected route
* luhbike/osm_util.py: OSM Nominatim utility
* luhbike/regression.py: regression of the selection probability
* luhbike/simra_loader.py: loading SimRa data
* luhbike/weather_loader.py: loading weather data

## Data Source

* [Open data of Berlin provided by the BTW 2023 Committee](https://scads.ai/wp-content/uploads/Datengrundlagen_Dokument.pdf)
* [SimRa usage data and incident data](https://www.digital-future.berlin/forschung/projekte/simra/)
* [Open hourly weather data of Berlin](https://www.dwd.de/DE/leistungen/klimadatendeutschland/klimadatendeutschland.html)
* [Tagesspiegel Radmesser](https://interaktiv.tagesspiegel.de/radmesser/)