import logging
from os import getenv

from dotenv import load_dotenv
from fastapi import FastAPI
from psycopg2 import connect

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


app = FastAPI(
    title="Store Monitoring",
    description="This is a store monitoring service.",
    version="0.1.0",
)

conn = connect(getenv("POSTGRESQL_URI"))
