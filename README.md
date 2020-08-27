# Data-relay

The purpose of this project is to create a local data source 
for local strategy development.

This project which will download the data from Alpha Vantage and
verify that data with our data server. As result, you will get
a local data, which you can use for local development.

## Setup instructions

At first, clone this project.

Then you need anaconda with python3. Install them.

After that, create a virual environment for this project:

```bash
$ conda create --name datarelay python=3.7 django=2.2.5 xarray=0.15 scipy=1.3.2 portalocker=1.5.2
```

Later, you can drop this environment if you don't need it or 
you want to reinstall it.

```bash
$ conda remove --name datarelay --all
```
 

## Configuration

Firstly, you need to register on alphavantage and get the api 
key for access to its api. Go and register here: 
https://www.alphavantage.co/support/#api-key

After that you need to create a file with name 'settings.py' 
in the root folder of this project. Write your configuration 
to this file. It should be something like this:
```python
AVANTAGE_KEY = "TheKeyFromAlphaVantage" # The key, which you got after registration on https://www.alphavantage.co
SYMBOLS = ['NASDAQ:AAPL', 'NASDAQ:FB']  # List of stock symbols or None to sync all 
INDEXES = ['SPX']                       # List of index symbols or None to sync all 
```

## Load data

```bash
conda activate datarelay
python manage.py assets_sync # stocks
python manage.py crypto_sync # stocks
python manage.py secgov_sync # fundamental
python manage.py idx_sync # indexes sync
python manage.py blsgov_sync # global data from bls.gov
```

It takes some time, especially if you set `SYMBOLS = None`. 
Roughly, it takes 20-30 seconds per asset (the main restriction is an rate limit of Alpha Vantage).
Also the synchronization of edgar database takes some time...

## Run the data server
```bash
conda activate datarelay # it may be not necessary, if you activated it before
python manage.py runserver 0.0.0.0:8000
```

## Use it!
That is all. Locally, the qnt library will use this data server. 

Sometimes, you have to write the right address of your data server 
into `DATA_BASE_URL` (environmental variable)  For example,
if you use another port number for the data server. 
You can set this variable in your script, 
but don`t forget remove that code, when you will deploy your code on quantnet.ai
```python
import os
os.environ["DATA_BASE_URL"] = 'http://127.0.0.1:8000/'
```
