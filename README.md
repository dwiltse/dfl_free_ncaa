# dfl_free_ncaa

This project collects NCAA football play-by-play data using the cfbd API and saves it as parquet files. 

## Setup
- Place your API key in a `.env` file as `CFBD_API_KEY=your_api_key_here`.
- Install dependencies: `pip install -r requirements.txt`
- Run the main script to collect and save data in the `cfb_data` folder.

## Output
- Parquet files will be saved in the `cfb_data` directory.
- The `.env` file and `cfb_data` directory are excluded from version control.
