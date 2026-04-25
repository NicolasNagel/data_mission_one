import requests
import pandas as pd

from src.backend.api import API

def main():
    api = API()
    df = api.fetch_data()
    return print(df)

if __name__ == '__main__':
    main()