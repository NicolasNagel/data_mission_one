import os
import pandas as pd
import logging

from typing import Optional

from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema

from dotenv import load_dotenv

from src.utils.control_classes import DataBaseResult


logger = logging.getLogger(__name__)
load_dotenv()

class DataBase:
    """Responsável por fazer todo o gerenciamento do Banco de Dados."""
    def __init__(self):
        self.db_user = os.getenv('DB_USER')
        self.db_pass = os.getenv('DB_PASS')
        self.db_host = os.getenv('DB_HOST')
        self.db_port = os.getenv('DB_PORT')
        self.db_name = os.getenv('DB_NAME')

        self.engine = create_engine(
            f'postgresql://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}'
        )

        self.Session = sessionmaker(bind=self.engine)

    def create_schema(self, schema: Optional[str] = 'bronze') -> None:
        try:
            with self.engine.connect() as conn:
                if not inspect(conn).has_schema(schema):
                    conn.execute(CreateSchema(schema, if_not_exists=True))
            logger.info(f'Schema {schema} criado com sucesso.')
        except Exception as e:
            logger.error(f'Erro ao criar schema: {schema}: {str(e)}')

    def insert_data(self, data: pd.DataFrame) -> DataBaseResult:
        """Insere os dados no Banco de Dados.
        
        Args:
            data (pd.DataFrame): DataFrame contendo o conteúdo a ser inserido.

        Returns:
            DataBaseResult: Mensagem de sucesso, se erro, mensagem de erro.
        """
        if data.empty:
            raise ValueError('DataFrame não foi passado ou está vazio. Inserção cancelada.')
        
        try:
            with self.engine.connect() as conn:
                data.to_sql(
                    name='bronze_api_sales',
                    con=conn,
                    schema='bronze',
                    if_exists='replace',
                    index=False
                )
                conn.commit()
            logger.info(f'{len(data)} arquivo(s) inserido(s) no Banco de Dados.')
            return DataBaseResult(
                success=True,
                data=len(data),
                error=None
            )
        except Exception as e:
            logger.error(f'Erro ao inserir dados no Banco de Dados: {str(e)}')
            return DataBaseResult(
                success=False,
                data=len(data),
                error=f'{str(e)}'
            )