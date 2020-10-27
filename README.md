# SarcQuoteDetectionRus
> Sarcasm and irony detection in quotes (Russian language)

The project is dedicated to collecting, analyzing,evaluating data for the 
task of sarcasm and irony detection in Russian. The data sources are citaty.info
and bbf.ru/quotes.

## Packages / Installing
* python 3.8
* scrapy, json - for collecting
* pandas, nltk, pymystem3, sklearn, seaborn - for analyzing and evaluating 

Install dependencies:
``` bash
pip3 install -r requirements.txt
```

## Data

You can download the data here:
https://drive.google.com/drive/folders/1D920QOEqyXekE9wDHWydyBpuvpqyCksh

Place data in the __results__ directory.

Read description.txt to get more information.

## Getting started

The package structure:

```
SarcQuoteDetectionRus
  |- notebooks
  | |- create_bbf_dataset.ipynb
  | |- create_citaty_info_dataset.ipynb
  | |- create_final_datasets.ipynb
  | |- get_datasets_metrics.ipynb
  |- parsers
  | |- spiders
  | | |- __inint__.py
  | | |- bbf_spider.py
  | | |- citaty_info_qbq_spider.py
  | | |- citaty_info_spider.py
  | |- __inint__.py
  | |- items.py
  | |- middlewares.py
  | |- pipelines.py
  | |- settings.py
  |- results
  | |- ...
  |- create_bbf_dataset.py
  |- create_citaty_info_dataset.py
  |- create_final_datasets.py
  |- get_datasets_metrics.py
  |- log.txt
  |- README.md
  |- requirements.txt
  |- scrapy.cfg
```

The directories:
* notebooks - ipynb directory
* parsers - scrapy directory
* results - data directory

### Parsers

* __bbf_spider.py__ - parser for collecting data from bbf.ru/quotes
* __citaty_info_qbq_spider.py__ - parser for collecting topic data from citaty.info/category/topic
* __citaty_info_spider.py__ - parser for collecting data from range of pages citaty.info/quote/page

To launch spider (from ../parsers/):
``` bash
scrapy crawl spider_name
```

Names of spiders:
* bbf
* citaty_info_qbq
* citaty_info

### Notebooks

Notebooks containing code for processing data, collecting a dataset, creating and evaluating models.
Each .ipynb file is associated with a corresponding .py file:

* create_bbf_dataset.py <--> create_bbf_dataset.ipynb
* create_citaty_info_dataset.py <--> create_citaty_info_dataset.ipynb
* create_final_datasets.py <--> create_final_datasets.ipynb
* get_datasets_metrics.py <--> get_datasets_metrics.ipynb

.py files run without parameters:
``` bash
python3 name_of_file.py
```

## Acknowledgments
We express our gratitude to the administration of citaty.info and bbf.ru for providing the data.
