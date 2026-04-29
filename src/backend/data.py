import os
import pandas as pd
import requests
import logging

from datetime import datetime
from typing import Optional
from dotenv import load_dotenv

from src.database.db import DataBase
from src.utils.control_classes import APIResult

logger = logging.getLogger(__name__)

load_dotenv()


class API:
    """
    Cliente responsável por consultar a API da DataMission, salvar a resposta bruta
    em disco e retornar os dados em formato tabular.
    """

    def __init__(self, db: Optional[DataBase] = None) -> None:
        self.db = db or DataBase()

        self.url = os.getenv("API_URL")
        self.api_key = os.getenv("API_KEY")
        self.params = {"format": "json"}
        self.timeout = 30
        self.headers = {
            "Authorization": f"Bearer {self.api_key}"
        }

    def get_data(self) -> APIResult:
        """
        Executa a chamada à API, salva a resposta JSON

        Raises:
            ValueError: Quando `API_URL` ou `API_KEY` não estão configuradas.
            RuntimeError: Quando ocorre erro HTTP, erro de comunicação ou erro
                ao interpretar a resposta como JSON.

        Returns:
            APIResult: Conteúdo JSON para tratamento posterior
        """
        if not self.url:
            raise ValueError("A variável de ambiente API_URL não está configurada.")

        if not self.api_key:
            raise ValueError("A variável de ambiente API_KEY não está configurada.")
        
        try:
            response = requests.get(
                url=self.url,
                headers=self.headers,
                params=self.params,
                timeout=self.timeout
            )
            response.raise_for_status()
            logger.info(f'Status retorando pela API: {response.status_code}')

            return APIResult(
                success=True,
                data=response.json(),
                status_code=response.status_code
            )
        
        except requests.HTTPError as e:
            status_code = e.response.status_code if e.response else None

            return APIResult(
                success=False,
                error=f'Erro HTTP ao buscar dados da API: {str(e)}',
                status_code=status_code
            )
        
        except requests.RequestException as e:
            return APIResult(
                success=False,
                error=f'Erro de comunicação ao buscar dados da API: {str(e)}'
            )
        
        except ValueError as e:
            return APIResult(
                success=False,
                error=f'Resposta da API não contém um JSON válido: {str(e)}'
            )
        
    def transform_data(self, content: APIResult) -> pd.DataFrame:
        """Transforma o arquivo JSON em uma DataFrame do Pandas.
        
        Args:
            content (APIResult): Conteúdo JSON da API.

        Returns:
            pd.DataFrame: DataFrame para visão tabular dos dados.
        """
        logger.info('Iniciando Transformação de Dados...')

        if not content.success:
            raise ValueError(f'Erro na consulta API. Transformação cancelada: {content.error}')
        

        if content.status_code != 200:
            raise ValueError(f'Status Code inválido: {content.status_code}. Transformação cancelada.')
        
        if not content.data:
            raise ValueError('Nenhum dado retornado na API. Transformação cancelada.')
        
        try:
            df = pd.DataFrame(content.data)

            df['sistem_source'] = 'api_data_mission'
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df['inserted_at'] = datetime.now()

            logger.info(f'Transformação concluída {len(df)} registro(s) transformado(s)')

            return df
        except Exception as e:
            logger.error(f'Erro ao transformar Dados: {str(e)}')
            raise ValueError(f'Não foi possível transformar o DataFrame')
        
    def normalize_names(self, data: pd.DataFrame) -> pd.DataFrame:
        """Padroniza os nomes dos arquivos para inserção no Banco de Dados.
        
        Args:
            data (pd.DataFrame): DataFrame para visão tabular dos dados.

        Returns:
            pd.DataFrame: DataFrame para visão tabular dos dados com o nome padronizado.
        """
        if data.empty:
            raise ValueError('DataFrame vazio. Normalização cancelada')

        prefix = 'bronze_api_'

    def save_data(self, data: pd.DataFrame) -> None:
        try:
            self.db.insert_data(data)
            logger.info('Dados salvos com sucesso.')
        except Exception as e:
            logger.error(f'Erro ao salvar os dados: {str(e)}')
            raise