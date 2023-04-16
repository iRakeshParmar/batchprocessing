from __future__ import annotations
from sqlalchemy import Column, MetaData, String, Table, Integer,VARCHAR,BigInteger,DECIMAL,DateTime,PrimaryKeyConstraint, Time
from sqlalchemy.orm import registry
from sqlalchemy.dialects.mssql import DATETIME2
from typing import Optional
from dataclasses import dataclass, field
from datetime import datetime
import pytz as tz

metadata = MetaData()
mapper_registry = registry()
metadata = mapper_registry.metadata


@mapper_registry.mapped
@dataclass
class StockMain:
    __tablename__ = 'StockMain'
    __table_args__ = (
        PrimaryKeyConstraint('Symbol', name='PK__StockMai__B7CC3F00D98243EF'),
    )
    __sa_dataclass_metadata_key__ = 'sa'

    def __init__(self, list):
        
        # self.Symbol = list.Symbol
        # self.Nasdaq_Traded = list.Nasdaq_Traded
        # self.Security_Name = list.Security_Name
        # self.Listing_Exchange = list.Listing_Exchange
        # self.Market_Category = list.Market_Category
        # self.ETF = list.ETF
        # self.Round_Lot_Size = list.Round_Lot_Size
        # self.Test_Issue = list.Test_Issue
        # self.Financial_Status = list.Financial_Status
        # self.CQS_Symbol = list.CQS_Symbol
        # self.NASDAQ_Symbol = list.NASDAQ_Symbol
        # self.NextShares = list.NextShares

        self.Nasdaq_traded = list[0]
        self.Symbol = list[1]
        self.Security_name = list[2]
        self.Listing_Exchange = list[3]
        self.Market_Category = list[4]
        self.ETF = list[5]
        self.Round_Lot_Size = list[6]
        self.Test_Issue = list[7]
        self.Financial_Status = list[8]
        self.CQS_Symbol = list[9]
        self.NASDAQ_Symbol = list[10]
        self.NextShares = list[11]

    Symbol: str = field(metadata={'sa': Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))})
    Nasdaq_Traded: Optional[str] = field(default=None, metadata={'sa': Column(String(5, 'SQL_Latin1_General_CP1_CI_AS'))})
    Security_Name: Optional[str] = field(default=None, metadata={'sa': Column(String(500, 'SQL_Latin1_General_CP1_CI_AS'))})
    Listing_Exchange: Optional[str] = field(default=None, metadata={'sa': Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))})
    Market_Category: Optional[str] = field(default=None, metadata={'sa': Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))})
    ETF: Optional[str] = field(default=None, metadata={'sa': Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))})
    Round_Lot_Size: Optional[int] = field(default=0, metadata={'sa': Column(Integer)})
    Test_Issue: Optional[str] = field(default=None, metadata={'sa': Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))})
    Financial_Status: Optional[str] = field(default=None, metadata={'sa': Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))})
    CQS_Symbol: Optional[str] = field(default=None, metadata={'sa': Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))})
    NASDAQ_Symbol: Optional[str] = field(default=None, metadata={'sa': Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))})
    NextShares: Optional[str] = field(default=None, metadata={'sa': Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))})


t_StockMain_dtype = {'Nasdaq_Traded': VARCHAR(length=5),
                    'Symbol':  VARCHAR(length=50),
                    'Security_Name': VARCHAR(length=500),
                    'Listing_Exchange': VARCHAR(length=50),
                    'Market_Category': VARCHAR(length=50),
                    'ETF': VARCHAR(length=50),
                    'Round_Lot_Size': Integer,
                    'Test_Issue': VARCHAR(length=50),
                    'Financial_Status': VARCHAR(length=50),
                    'CQS_Symbol': VARCHAR(length=50),
                    'NASDAQ_Symbol': VARCHAR(length=50),
                    'NextShares': VARCHAR(length=50),
                    }


t_StockDetail = Table(
    'StockDetail', metadata,
    Column('Symbol', String(50, 'SQL_Latin1_General_CP1_CI_AS')),
    Column('Date', DateTime),
    Column('Open', DECIMAL(18, 8)),
    Column('High', DECIMAL(18, 8)),
    Column('Low', DECIMAL(18, 8)),
    Column('Close', DECIMAL(18, 8)),
    Column('Adj Close', DECIMAL(18, 8)),
    Column('Volume', BigInteger)
)

t_StockDetail_dtype = {'Symbol':  VARCHAR(length=50),
                    'Date': DateTime,
                    'Open': DECIMAL(18, 8),
                    'High': DECIMAL(18, 8),
                    'Low': DECIMAL(18, 8),
                    'Close': DECIMAL(18, 8),
                    'Adj Close': DECIMAL(18, 8),
                    'Volume': BigInteger,
                    }

t_BatchProcessStats_dtype = {'Batchtype': VARCHAR(length=100),
                    'StartTime': DateTime,
                    'Endtime': DateTime,
                    'TimeTaken': DateTime}                


@mapper_registry.mapped
@dataclass
class BatchProcessStats:
    __tablename__ = 'BatchProcessStats'
    __table_args__ = (
        PrimaryKeyConstraint('Batchtype', 'StartTime', 'Endtime', name='PK_BatchProcessStats'),
    )
    __sa_dataclass_metadata_key__ = 'sa'
    
    def __init__(self, batchtype,startTime,endtime,timeTaken):
        self.Batchtype = batchtype
        self.StartTime = startTime
        self.Endtime = endtime
        self.TimeTaken = datetime.min + timeTaken

    Batchtype: str = field(metadata={'sa': Column(String(100, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)})
    StartTime: datetime = field(metadata={'sa': Column(DateTime, nullable=False)})
    Endtime: datetime = field(metadata={'sa': Column(DateTime, nullable=False)})
    TimeTaken: Optional[datetime] = field(default=None, metadata={'sa': Column(DATETIME2)})