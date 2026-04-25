import os
import requests
import pandas as pd

from typing import Dict, Any
from dotenv import load_dotenv

load_dotenv()

class API:
    """Controla as Requisições da API."""

    def __init__(self) -> None:
        self.url = os.getenv('API_URL')
        self.api_key = os.getenv('API_KEY')
        self.params = 'format=json'
        self.headers = {
            'Authorization': f'Bearer {self.api_key}'
        }

    def get_data(self) -> Dict[str, Any]:
        """
        Faz a requisição na URL da API da DataMission e retorna os dados em formato JSON.

        Returns:
            Dict(str, Any): Conteúdo em JSON da chamada da API.
        """
        try:
            response = requests.get(url=self.url, headers=self.headers, params=self.params)
            response.raise_for_status()

            content = response.json()
            return content
        except Exception as e:
            print(f'Erro ao buscar os dados da API: {str(e)}')
    
    def fetch_data(self, content: Dict[str, Any]) -> pd.DataFrame:
        """
        Transforma o arquivo JSON em um DataFrame do Pandas.

        Args:
            content(Dict[str, Any]): Conteúdo em JSON da chamada da API.

        Returns:
            pd.DataFrame: DataFrame do Pandas com os dados em formato tabular.
        """

        if not content:
            raise ValueError('content não pode ser vazio ou None.')
        
        try:
            df = pd.DataFrame(content)
            return df
        except Exception as e:
            print(f'Erro ao transformar o conteúdo JSON em DataFrame: {str(e)}')