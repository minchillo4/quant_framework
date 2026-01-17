# src/quant_framework/storage/bronze/raw_writer.py
class RawBronzeWriter:
    """Apenas escreve dados BRUTOS no MinIO - ZERO transformação"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3', ...)
    
    async def write_raw_data(
        self,
        source: str,                # Ex: "coinmetrics", "coinalyze", "ccxt"
        data_type: str,             # Ex: "ohlc", "open_interest", "onchain"
        symbol: str,                # Ex: "BTC", "ETH"
        raw_data: dict | list,      # Dados BRUTOS da API
        timestamp: datetime = None
    ) -> dict:
        """Escreve dados brutos no bucket bronze"""
        
        # 1. Estrutura de pastas padronizada
        partition_path = self._build_partition_path(
            source=source,
            data_type=data_type,
            symbol=symbol,
            timestamp=timestamp or datetime.utcnow()
        )
        
        # 2. Serializa RAW data (sem validação!)
        if isinstance(raw_data, dict):
            raw_bytes = json.dumps(raw_data).encode('utf-8')
        else:
            raw_bytes = pickle.dumps(raw_data)  # Ou msgpack/avro
        
        # 3. Upload para MinIO
        s3_key = f"{partition_path}/raw_{int(time.time())}.bin"
        self.s3_client.put_object(
            Bucket="bronze",
            Key=s3_key,
            Body=raw_bytes,
            Metadata={
                'source': source,
                'data_type': data_type,
                'symbol': symbol,
                'timestamp': timestamp.isoformat() if timestamp else '',
                'format': 'raw_json' if isinstance(raw_data, dict) else 'raw_pickle'
            }
        )
        
        return {"s3_key": s3_key, "bytes_written": len(raw_bytes)}
