import requests
import pandas as pd

from src.backend.api import API

def main():
    api = API()
    content = api.get_data()
    df = api.fetch_data(content)
    return print(df)

if __name__ == '__main__':
    main()