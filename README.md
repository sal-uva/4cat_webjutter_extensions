# Webjutter extensions for 4CAT
This extension forms the bridge between [4CAT](https://github.com/digitalmethodsinitiative/4cat) and [Webjutter](https://github.com/digitalmethodsinitiative/4cat-scrapers), a tool for data collection and search. It requires running 4CAT and Webjutter instances to work. This repo consists of:
- *Webjutter datasource*: A datasource for 4CAT that lets you search through a collection and write the results to a dataset. It interfaces with Webjutter's `api/[collection/search` endpoint, using ElasticSearch query strings. Webjutter collections become their own datasource types in 4CAT.
- *Webjutter worker*: A worker for 4CAT that periodically checks whether 4CAT can connect to Webjutter, given the URL, user, and password as defined in the settings. If so, it retrieves the data from Webjutter's `api/overview` endpoint and saves this data to 4CAT's `config/extensions` directory. This file is then used in the Webjutter datasource options.
- *Explorer templates*: Various 4CAT Explorer templates for distinct collections in Webjutter.

### How to install
1. In 4CAT, go to `Control panel -> Extensions`.
2. Insert the URL of this repository in 'Repository URL'.
3. Click 'Install'. The extension should be installed and 4CAT will be restarted. See the log on the bottom of this page for its status. You may have to restart manually.
      - Alternatively, `Control Panel -> Extensions` also lets you upload this repo as a .zip, or you can manually create a git repository for this project in 4cat's config/extensions folder yourself (see `README.md` in 4cat's `config/extensions` directory).
4. Go to `Control Panel -> Settings -> Extensions`. Make sure 'Webjutter datasource and worker' is selected, and restart 4CAT (Control Panel -> Settings -> Restart or Upgrade).
5. Go to `Control Panel -> Settings -> Datasources` and enable Webjutter. You may have to enable/disable this datasource for different user tags.
6. Go to `Control Panel -> Settings -> Webjutter search` and insert the URL where you can reach your Webjutter, as well as the username and passwork. These are defined in the `.env` file of Webjutter. Press 'Save settings'.
7. Go to `Create dataset -> Webjutter` and see if it shows your Webjutter data. The Webjutter worker may need some time to fetch the latest `api/overview` data.

