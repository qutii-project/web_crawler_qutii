# web_crawler_qutii
Repositiory for web crawler


### Codebase structure:

#### 1. crawler_init.py

   wrapper script to which takes in the keywords and publisher details to call the respective publisher functions.
   
#### 3. publishers/

   Directory which contains the code logic for each publisher. Each new addition of publisher in the future will require a definition/ API logic implementation in this directory
   
#### 4. tiquu_pg

Directory which contains the utility scripts to handle postgres connections and IUD operations
`tiquu_pg.select_query`
`tiquu_pg.insert_query`

#### 5. Config files:

- api_env.yaml
 Config file for API peamters
Template: 

publisher name:
api_required_boolean (paramter True if api key is required else False)
api_key: (holds api key if api_required_boolean paramter is True)
base_url: this is required paramter of the base url of publisher API (from documentation)
result_format: the format in which data is expected 
solr: set to True of API query is build using solr (refer to online documentation of solr)

- database.ini
  to maintain postgres connection details
