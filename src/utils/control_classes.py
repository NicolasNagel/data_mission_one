from dataclasses import dataclass
from typing import Optional, List, Dict, Any

@dataclass
class APIResult:
    """
    Classe responsável por mapear os resultados do retorno da API.
    """
    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None
    status_code: Optional[int] = None

@dataclass
class DataBaseResult:
    """
    Classe responsável por controlar o fluxo de ingestão no Banco de Dados.
    """
    success: bool
    data: Optional[int] = None
    error: Optional[str] = None

@dataclass
class PipelineResult:
    """
    Classe responsável por controlar o fluxo da Pipeline.
    """
    success: bool
    data: Optional[int] = None
    error: Optional[str] = None