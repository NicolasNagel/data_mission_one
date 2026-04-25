import json
import os
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import requests
from fastapi import FastAPI, HTTPException


app = FastAPI()


class API:
    """
    Cliente responsável por consultar a API da DataMission, salvar a resposta bruta
    em disco e retornar os dados em formato tabular.
    """

    def __init__(self) -> None:
        self.url = os.getenv("API_URL")
        self.api_key = os.getenv("API_KEY")
        self.params = {"format": "json"}
        self.headers = {
            "Authorization": f"Bearer {self.api_key}"
        }

    def fetch_data(self) -> pd.DataFrame:
        """
        Executa a chamada à API, salva a resposta JSON em disco e retorna os dados
        em formato tabular.

        Raises:
            ValueError: Quando `API_URL` ou `API_KEY` não estão configuradas.
            RuntimeError: Quando ocorre erro HTTP, erro de comunicação ou erro
                ao interpretar a resposta como JSON.

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
                timeout=30,
            )

            print(f"Status retornado pela API: {response.status_code}")

            response.raise_for_status()
            content = response.json()

            with open("raw_data.json", "w", encoding="utf-8") as file:
                json.dump(content, file, ensure_ascii=False, indent=2)

            print("Arquivo raw_data.json salvo com sucesso.")

            data = content.get("data", content) if isinstance(content, dict) else content

            df = pd.DataFrame(data)

            print("Primeiros 5 registros extraídos da API:")
            print(df.head())

            return df

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


def process_inventory(file_path: str = "raw_data.json") -> Dict[str, Any]:
    """
    Lê o arquivo raw_data.json, normaliza os dados com Pandas e calcula métricas
    de inventário por categoria.

    A função carrega o JSON bruto salvo em disco, transforma os registros em um
    DataFrame, normaliza os nomes das colunas e calcula métricas agregadas como
    quantidade total, quantidade média e total de registros por categoria.

    Args:
        file_path (str): Caminho do arquivo JSON bruto salvo pela etapa de extração.

    Raises:
        FileNotFoundError: Quando o arquivo informado não existe.
        ValueError: Quando o arquivo está vazio, inválido ou sem as colunas esperadas.

    Returns:
        Dict[str, Any]: Métricas agregadas de inventário.
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

    with open(path, "r", encoding="utf-8") as file:
        content = json.load(file)

    data = content.get("data", content) if isinstance(content, dict) else content

    if not data:
        raise ValueError("O arquivo raw_data.json não contém dados para processamento.")

    df = pd.json_normalize(data)

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace(".", "_", regex=False)
    )

    print("Primeiros 5 registros normalizados para validação:")
    print(df.head())

    required_columns = {"product_category", "quantity"}
    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Colunas obrigatórias ausentes no arquivo: {', '.join(missing_columns)}"
        )

    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

    if df["quantity"].isna().any():
        raise ValueError("A coluna quantity possui valores inválidos ou nulos.")

    metrics_by_category = (
        df.groupby("product_category", as_index=False)
        .agg(
            total_quantity=("quantity", "sum"),
            average_quantity=("quantity", "mean"),
            total_records=("quantity", "count"),
        )
        .sort_values("product_category")
    )

    return {
        "total_quantity": int(df["quantity"].sum()),
        "average_quantity": float(df["quantity"].mean()),
        "total_records": int(df["quantity"].count()),
        "categories": metrics_by_category.to_dict(orient="records"),
    }


@app.get("/")
def root() -> Dict[str, str]:
    """
    Endpoint raiz da aplicação.

    Returns:
        Dict[str, str]: Mensagem informando a rota principal da API.
    """
    return {
        "message": "API rodando. Acesse /inventory para consultar as métricas."
    }


@app.get("/inventory")
def get_inventory() -> Dict[str, Any]:
    """
    Retorna métricas de inventário calculadas a partir do arquivo raw_data.json.

    Returns:
        Dict[str, Any]: Métricas agregadas por categoria.
    """
    try:
        return process_inventory("raw_data.json")

    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Erro inesperado ao processar inventário: {exc}",
        ) from exc