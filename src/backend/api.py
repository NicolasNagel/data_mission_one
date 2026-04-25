import os
import json
import requests
import pandas as pd

from dataclasses import dataclass
from typing import Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()

@dataclass
class APIResult:
    """
    Resultado padronizado de uma chamada à API.

    Permite que o chamador trate falhas de forma previsível, verificando
    o atributo `success` antes de acessar os dados retornados.

    Attributes:
        success: Indica se a chamada foi concluída com sucesso.
        data: Conteúdo JSON retornado pela API em caso de sucesso.
        error: Mensagem de erro em caso de falha.
        status_code: Código HTTP da resposta, quando disponível.
    """
    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    status_code: Optional[int] = None

class API:
    """
    Cliente responsável por consultar a API da DataMission e converter a resposta em DataFrame.

    A classe centraliza a configuração da requisição HTTP, incluindo URL, chave de API,
    parâmetros de consulta e cabeçalhos de autenticação. As configurações são carregadas
    a partir das variáveis de ambiente `API_URL` e `API_KEY`.

    Methods:
        get_data:
            Executa a requisição HTTP para a API e retorna um `APIResult`
            com o resultado padronizado da chamada.

        fetch_data:
            Converte os dados retornados pela API em um `pd.DataFrame`.

    Raises:
        ValueError: Quando `fetch_data` recebe um conteúdo vazio, nulo ou inválido.
    """
    def __init__(self) -> None:
        self.url = os.getenv('API_URL')
        self.api_key = os.getenv('API_KEY')
        self.params = 'format=json'
        self.headers = {
            'Authorization': f'Bearer {self.api_key}'
        }

    def get_data(self) -> APIResult:
        """
        Executa uma requisição GET para a API da DataMission.

        Returns:
            APIResult: Objeto contendo o status da operação, os dados retornados,
            a mensagem de erro e o código HTTP, quando disponíveis.
        """
        try:
            response = requests.get(
                url=self.url,
                headers=self.headers,
                params=self.params,
                timeout=30
            )
            response.raise_for_status()

            return APIResult(
                success=True,
                data=response.json(),
                status_code=response.status_code
            )
        
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response else None

            return APIResult(
                success=False,
                error=f'Erro HTTP ao buscar dados da API: {exc}',
                status_code=status_code
            )
        
        except requests.RequestException as exc:
            return APIResult(
                success=False,
                error=f'Erro de comunicação ao buscar dados da API: {exc}'
            )
        
        except ValueError as exc:
           return APIResult(
                success=False,
                error=f'Resposta da API não contém JSON válido: {exc}'
            )
    
    def fetch_data(self, content: Dict[str, Any]) -> pd.DataFrame:
        """
        Converte os dados de um `APIResult` bem-sucedido em um DataFrame do Pandas.

        Args:
            result (APIResult): Resultado retornado pelo método `get_data`.

        Raises:
            ValueError: Quando o resultado indica falha ou não contém dados válidos.

        Returns:
            pd.DataFrame: DataFrame contendo os dados retornados pela API.
        """
        if not content.success:
            raise ValueError(f'Não foi possível obter dados da API {content.error}.')
        
        if not content.data:
            raise ValueError('O resultado da API não contém dados para conversão.')
        
        try:
            with open(f'raw_data.json', 'w', encoding='utf-8') as file:
                json.dump(content.data, file, ensure_ascii=False, indent=2)

            return print(f'Arquivo escrito e salvo localmente com sucesso.')
        
        except Exception as e:
            raise ValueError(
                f'Erro ao transformar conteúdo JSON em um arquivo: {str(e)}'
            ) from e