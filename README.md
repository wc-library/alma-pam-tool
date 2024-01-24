# alma-pam-tool
Bulk update Public Access Models via the Alma API

## Installation/Configuration
### Installation
1. Clone or download this repository to your machine. `git clone git@github.com:wc-library/alma-pam-tool.git` or click "Code" then "Download Zip"
2. Install the python dependency modules `pip install -r requirements.txt` from inside the alma-pam-tool directory.

### Constant Configuration
1. Set the `APIKEY` constant to an Ex Libris Developer API key with Electronic Resources read/write permision - unless you only need to use the `review` mode, in which case the read permission is sufficient.
2. Update the `BASEURL` constant to match your Alma instance's API endpoint. Consult [https://developers.exlibrisgroup.com/alma/apis/](https://developers.exlibrisgroup.com/alma/apis/)
3. Set the `MAX_API_CALLS_PER_DAY` constant to an integer that makes sense for your institution's API limits and existing usage. Check [https://developers.exlibrisgroup.com/manage/reports/](https://developers.exlibrisgroup.com/manage/reports/) to see your API Threshold and usage.

#### Example of configuration set-up:
![configuration](https://github.com/wc-library/alma-pam-tool/assets/64615625/a5947865-afe4-48e2-88d6-eb2d6973e2c5)

## Usage

The tool allows for bulk review and updating of the public access models of a particular collection in Alma. Each time you want to run it you need set certain configurations. 

### Per Run Configuration
1. Find the section in `main.py` with the comment heading "Per Run Configuration". 
2. Set your mode. Read more about each mode below.
3. Set the collection ID for the collection that you'll be working with.
4. Set the service ID for the collection that you'll be working with.
5. Set the code and the description for the Public Access Model that you want to change undefined or empty PAMs to use. This will only matter in the `update` mode. These can be found in Alma in Configuration > Acquisitions > Licenses > Access Model

### Running the tool
Once the installation, the constants configuration, and the per run configuration are done, you can run the script by opening a terminal window in the alma-pam-tool directory and typing `python main.py`

![alma-pam-tool_1](https://github.com/wc-library/alma-pam-tool/assets/64615625/c6686e30-4efc-4c73-9f9a-eac6635489b7)

The script will provide status updates as it runs. When it finishes it will also create a timestamped report file based on the mode as well as an error log file if any errors were encountered. 

#### Examples of program progress:
![alma-pam-tool_2](https://github.com/wc-library/alma-pam-tool/assets/64615625/13a3abfd-ed26-4d69-a904-c66f7405f08b)
![alma-pam-tool_3](https://github.com/wc-library/alma-pam-tool/assets/64615625/a47c374b-580b-4c22-9934-594f2f751ae5)

> [!NOTE]
> The numbers and percentages that appear on the left-hand side of each of the progress lines is out of order because the program runs asynchronously. These percentages more accurately reflect progress with larger collections.

A cache file will also be created if one did not exist yet. This will help reduce unnecessary API requests. By default the cache data will expire for a particular item after one week. You may use one of the cache clearing modes if you wish to immediately fetch the portfolio/collection information again. 

The cache also keeps a record of the API requests that have been made and will remove those after midnight GMT when Ex Libris resets the daily threshold. These do not get cleared from the cache in the cache clearing modes since Ex Libris obviously will not have reset their count early. 

### Review Mode

The review mode retrieves portfolio information from the API or the cache, pertaining to the collection ID, and reports their titles and public access models, in a log file sorted by the current PAM values. This is a useful first step in preparing to update the PAMs. 

#### Review mode log example:
![alma-pam-tool_4](https://github.com/wc-library/alma-pam-tool/assets/64615625/e19e683e-09d4-4967-afec-812165ebf55c)

### Update Mode

The update mode retrieves portfolio information from the API or the cache for a particular collection ID and then update empty or undefined PAMs with the desired new PAM value in Alma. 

### Cache Clearing Modes
- clear_cache_all
  
  Removes all cached data except for the API request count from the past 24 hours
- clear_cache_collection
  
  Removes all cached data for a particular collection ID
- clear_cache_portfolios
  
  Removes a particular portfolio from the cache by the portfolio's ID within the current collection ID. The script will prompt for the portfolio ID as it is running. 
