import os

os.environ['KAGGLE_CONFIG_DIR'] = 'kaggle'

os.system('kaggle datasets download -d jacksoncrow/stock-market-dataset')

os.system('unzip stock-market-dataset.zip')
