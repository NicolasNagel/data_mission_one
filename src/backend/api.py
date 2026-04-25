import os
import json
import requests
import pandas as pd

from typing import Dict, Any
from dotenv import load_dotenv

load_dotenv()

class API:
    """
    Cliente responsável por consultar a API da DataMission, salvar a resposta bruta
    em disco e retornar os dados em formato tabular.

    A classe centraliza a configuração da requisição HTTP, incluindo URL, chave de API,
    parâmetros de consulta e cabeçalhos de autenticação. As configurações são carregadas
    a partir das variáveis de ambiente `API_URL` e `API_KEY`.

    O método `fetch_data` executa a chamada HTTP, valida a resposta, converte o conteúdo
    retornado para JSON, salva os dados no arquivo `raw_data.json` e retorna um
    `pd.DataFrame` para uso nas próximas etapas da ETL.

    Attributes:
        url (str | None): URL da API carregada da variável de ambiente `API_URL`.
        api_key (str | None): Chave de autenticação carregada da variável de ambiente `API_KEY`.
        params (str): Parâmetros enviados na requisição à API.
        headers (dict): Cabeçalhos HTTP utilizados na requisição, incluindo autenticação.

    Methods:
        fetch_data:
            Executa a requisição à API, salva a resposta JSON em disco e retorna
            os dados como um `pd.DataFrame`.

    Raises:
        ValueError: Quando `API_URL` ou `API_KEY` não estão configuradas.
        RuntimeError: Quando ocorre erro HTTP, erro de comunicação ou falha ao
            interpretar a resposta como JSON.
    """
    def __init__(self) -> None:
        self.url = os.getenv('API_URL')
        self.api_key = os.getenv('API_KEY')
        self.params = 'format=json'
        self.headers = {
            'Authorization': f'Bearer {self.api_key}'
        }
    
    def fetch_data(self) -> pd.DataFrame:
        """
        Executa a chamada à API da DataMission, salva a resposta JSON e retorna os dados.

        A função realiza uma requisição GET usando a URL, os headers e os parâmetros
        configurados na instância. Em caso de sucesso, a resposta é convertida para JSON,
        salva no arquivo `raw_data.json` e transformada em um `pd.DataFrame`.

        Quando o JSON retornado possui uma chave `data`, essa chave é usada como origem
        dos registros. Caso contrário, o conteúdo completo da resposta é usado para criar
        o DataFrame.

        Raises:
            ValueError: Quando as variáveis de ambiente `API_URL` ou `API_KEY`
                não estão configuradas.
            RuntimeError: Quando ocorre erro HTTP, falha de comunicação com a API
                ou quando a resposta não contém um JSON válido.

        Returns:
            pd.DataFrame: DataFrame contendo os dados retornados pela API.
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
                timeout=30
            )
            response.raise_for_status()
            content = response.json()

            with open('raw_data.json', 'w', encoding='utf-8') as file:
                json.dump(content, file, ensure_ascii=False, indent=2)

            print('Arquivo raw_data.json salvo com sucesso.')

            data = content.get("data", content) if isinstance(content, dict) else content
            
            return pd.DataFrame(data)
        
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response else None
            raise RuntimeError(
                f"Erro HTTP ao buscar dados da API. "
                f"Status code: {status_code}. Erro original: {exc}"
            ) from exc

        except requests.RequestException as exc:
            raise RuntimeError(
                f"Erro de comunicação ao buscar dados da API: {exc}"
            ) from exc

        except ValueError as exc:
            raise RuntimeError(
                f"Resposta da API não contém um JSON válido: {exc}"
            ) from exc