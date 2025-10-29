"""
MongoDB Data Ingestion Script - Optimized for Fast On-the-Fly Aggregation
Run this script ONCE to load CSV data into MongoDB
Usage: python scripts/ingest_data.py
"""

import pandas as pd
import h3
from pymongo import MongoClient, ASCENDING
from datetime import datetime
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def parse_gps_coordinate(gps_string):
    """Parse GPS coordinate string like '13.014071,77.532051'"""
    try:
        if pd.isna(gps_string) or str(gps_string).strip() == '':
            return None, None
        parts = str(gps_string).strip().split(',')
        if len(parts) == 2:
            lat = float(parts[0].strip())
            lon = float(parts[1].strip())
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                return lat, lon
    except:
        pass
    return None, None

def create_indexes(collection):
    """Create optimized indexes for fast aggregation queries"""
    print("\n🔧 Creating MongoDB indexes for fast aggregation...")
    
    # Single field indexes
    collection.create_index([("logistics_player", ASCENDING)], background=True)
    collection.create_index([("hour_bin", ASCENDING)], background=True)
    collection.create_index([("order_status", ASCENDING)], background=True)
    collection.create_index([("h3_res_8", ASCENDING)], background=True)
    collection.create_index([("pickup_location", "2dsphere")], background=True)
    
    # CRITICAL: Compound indexes for fast filtered aggregation
    collection.create_index([
        ("logistics_player", ASCENDING),
        ("h3_res_8", ASCENDING)
    ], background=True)
    
    collection.create_index([
        ("hour_bin", ASCENDING),
        ("h3_res_8", ASCENDING)
    ], background=True)
    
    collection.create_index([
        ("logistics_player", ASCENDING),
        ("hour_bin", ASCENDING),
        ("h3_res_8", ASCENDING)
    ], background=True)
    
    print("Optimized indexes created for sub-second aggregation!")

def ingest_csv_to_mongodb(csv_path=None, chunk_size=None):
    """Load CSV data into MongoDB"""
    
    if csv_path is None:
        csv_path = Config.CSV_FILE_PATH
    if chunk_size is None:
        chunk_size = Config.CHUNK_SIZE
    
    print("=" * 80)
    print("LOGISTICS DATA INGESTION TO MONGODB")
    print("=" * 80)
    
    client = MongoClient(Config.MONGO_URI)
    db = client[Config.MONGO_DB_NAME]
    collection = db[Config.MONGO_COLLECTION_NAME]
    
    existing_count = collection.count_documents({})
    print(f"\nExisting records: {existing_count:,}")

    if existing_count >= Config.MIN_RECORDS_FOR_SKIP:
        print(f"Detected {existing_count:,} records. Skipping ingestion...")
        create_indexes(collection)
        
        print(f"\nDATABASE SUMMARY:")
        print(f"   Total documents: {collection.count_documents({}):,}")
        print(f"   Unique logistics players: {len(collection.distinct('logistics_player'))}")
        
        first_doc = collection.find_one(sort=[('timestamp', 1)])
        last_doc = collection.find_one(sort=[('timestamp', -1)])
        if first_doc and last_doc:
            print(f"   Date range: {first_doc['date']} to {last_doc['date']}")
        
        success_count = collection.count_documents({'order_status': 'success'})
        total_count = collection.count_documents({})
        if total_count > 0:
            print(f"   Success rate: {success_count / total_count * 100:.1f}%")
        
        print(f"\nReady for visualization!")
        print(f"   Run: python app.py")
        print("=" * 80)
        client.close()
        return

    elif existing_count > 0:
        print(f"\nDatabase already contains {existing_count:,} records")
        response = input("Delete existing data and reload? (yes/no): ")
        if response.lower() != 'yes':
            print("Ingestion cancelled")
            client.close()
            return
        
        print("🗑️  Dropping collection...")
        collection.drop()
        collection = db[Config.MONGO_COLLECTION_NAME]
    
    print(f"\n📂 Loading CSV: {csv_path}")
    print(f"⚙️  Chunk size: {chunk_size:,} rows\n")
    
    total_inserted = 0
    total_skipped = 0
    batch_records = []
    
    try:
        for chunk_num, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunk_size), 1):
            chunk.columns = chunk.columns.str.strip().str.lower()
            
            column_mapping = {
                'bpp_id': 'logistics_player',
                'timestamp': 'timestamp_raw',
                'pick_up_gps': 'pickup_gps',
                'delivery_gps': 'delivery_gps',
                'order_status': 'order_status'
            }
            chunk = chunk.rename(columns=column_mapping)
            
            chunk[['pickup_lat', 'pickup_lon']] = chunk['pickup_gps'].apply(
                lambda x: pd.Series(parse_gps_coordinate(x))
            )
            
            chunk[['delivery_lat', 'delivery_lon']] = chunk['delivery_gps'].apply(
                lambda x: pd.Series(parse_gps_coordinate(x))
            )
            
            chunk = chunk.dropna(subset=['pickup_lat', 'pickup_lon'])
            
            chunk['timestamp'] = pd.to_datetime(chunk['timestamp_raw'], format='mixed', errors='coerce')
            chunk = chunk.dropna(subset=['timestamp'])
            
            chunk['hour'] = chunk['timestamp'].dt.hour
            chunk['date'] = chunk['timestamp'].dt.date.astype(str)
            chunk['day_of_week'] = chunk['timestamp'].dt.dayofweek
            chunk['hour_bin'] = chunk['hour'].apply(lambda x: f"{x:02d}-{(x+1):02d}")
            
            chunk['order_status'] = chunk['order_status'].str.strip().str.lower()
            chunk['logistics_player'] = chunk['logistics_player'].fillna('unknown')
            
            for res in Config.H3_RESOLUTIONS:
                chunk[f'h3_res_{res}'] = chunk.apply(
                    lambda row: h3.latlng_to_cell(row['pickup_lat'], row['pickup_lon'], res=res),
                    axis=1
                )
            
            for _, row in chunk.iterrows():
                doc = {
                    'timestamp': row['timestamp'],
                    'date': row['date'],
                    'hour': int(row['hour']),
                    'hour_bin': row['hour_bin'],
                    'day_of_week': int(row['day_of_week']),
                    'pickup_lat': float(row['pickup_lat']),
                    'pickup_lon': float(row['pickup_lon']),
                    'pickup_location': {
                        'type': 'Point',
                        'coordinates': [float(row['pickup_lon']), float(row['pickup_lat'])]
                    },
                    'delivery_lat': float(row['delivery_lat']) if pd.notna(row['delivery_lat']) else None,
                    'delivery_lon': float(row['delivery_lon']) if pd.notna(row['delivery_lon']) else None,
                    'order_status': str(row['order_status']),
                    'logistics_player': str(row['logistics_player']),
                    **{f'h3_res_{res}': str(row[f'h3_res_{res}']) for res in Config.H3_RESOLUTIONS}
                }
                
                if doc['delivery_lat'] and doc['delivery_lon']:
                    doc['delivery_location'] = {
                        'type': 'Point',
                        'coordinates': [doc['delivery_lon'], doc['delivery_lat']]
                    }
                
                batch_records.append(doc)
                
                if len(batch_records) >= Config.BATCH_SIZE:
                    try:
                        collection.insert_many(batch_records, ordered=False)
                        total_inserted += len(batch_records)
                    except Exception as e:
                        print(f"⚠️  Batch insert warning: {e}")
                        total_skipped += len(batch_records)
                    batch_records = []
            
            print(f"✓ Chunk {chunk_num}: Processed (Total inserted: {total_inserted:,})")

            if chunk_num >= Config.MAX_CHUNKS:
                print(f"\nReached {Config.MAX_CHUNKS} chunks, stopping...\n")
                break
        
        if batch_records:
            try:
                collection.insert_many(batch_records, ordered=False)
                total_inserted += len(batch_records)
            except Exception as e:
                print(f"Final batch warning: {e}")
                total_skipped += len(batch_records)
        
        print(f"\n{'=' * 80}")
        print(f"DATA INGESTION COMPLETE!")
        print(f"{'=' * 80}")
        print(f"Total records inserted: {total_inserted:,}")
        if total_skipped > 0:
            print(f"Records skipped: {total_skipped:,}")
        
        create_indexes(collection)

        print(f"\nDATABASE SUMMARY:")
        print(f"   Total documents: {collection.count_documents({}):,}")
        print(f"   Unique logistics players: {len(collection.distinct('logistics_player'))}")
        
        first_doc = collection.find_one(sort=[('timestamp', 1)])
        last_doc = collection.find_one(sort=[('timestamp', -1)])
        if first_doc and last_doc:
            print(f"   Date range: {first_doc['date']} to {last_doc['date']}")
        
        success_count = collection.count_documents({'order_status': 'success'})
        total_count = collection.count_documents({})
        if total_count > 0:
            print(f"   Success rate: {success_count / total_count * 100:.1f}%")
        
        print(f"\nReady for visualization!")
        print(f"   Run: python app.py")
        print("=" * 80)
        
    except FileNotFoundError:
        print(f"Error: File '{csv_path}' not found!")
    except Exception as e:
        print(f"Error during ingestion: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()

if __name__ == '__main__':
    csv_file = None
    
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    
    ingest_csv_to_mongodb(csv_file)