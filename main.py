import logging

from src.backend.data import API

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
)

def pipeline():
    api = API()
    content = api.get_data()
    df = api.transform_data(content)
    api.save_data(df)

if __name__ == '__main__':
    pipeline()