"""Automatic snapshot management for Iceberg tables in TimeStream project."""

import logging
from datetime import datetime, timedelta
from typing import List, Optional

from pyspark.sql import SparkSession
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Snapshot

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SnapshotManager:
    """Manages Iceberg table snapshots with automatic cleanup."""
    
    def __init__(
        self,
        table_name: str,
        retention_days: int = 7,
        min_snapshots_to_keep: int = 3
    ):
        """Initialize SnapshotManager.
        
        Args:
            table_name: Full name of Iceberg table (e.g., 'nessie.timestream.cleaned_trips')
            retention_days: Number of days to retain snapshots
            min_snapshots_to_keep: Minimum number of recent snapshots to keep
        """
        self.table_name = table_name
        self.retention_days = retention_days
        self.min_snapshots_to_keep = min_snapshots_to_keep
        self.spark = self._create_spark_session()
        self.catalog = self._get_catalog()
        
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        return (SparkSession.builder
            .appName("Iceberg Snapshot Manager")
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .getOrCreate())
    
    def _get_catalog(self):
        """Get Iceberg catalog."""
        return load_catalog("nessie", **{
            "uri": "http://localhost:19120/api/v1",
            "default_branch": "main"
        })
    
    def get_table(self) -> Table:
        """Get Iceberg table."""
        return self.catalog.load_table(self.table_name)
    
    def list_snapshots(self) -> List[Snapshot]:
        """List all snapshots for the table."""
        table = self.get_table()
        return list(table.snapshots())
    
    def get_expired_snapshots(self) -> List[Snapshot]:
        """Get list of expired snapshots based on retention policy."""
        snapshots = self.list_snapshots()
        if not snapshots:
            return []
            
        # Sort snapshots by timestamp
        sorted_snapshots = sorted(
            snapshots,
            key=lambda s: s.timestamp_ms,
            reverse=True
        )
        
        # Keep minimum required snapshots
        protected_snapshots = sorted_snapshots[:self.min_snapshots_to_keep]
        candidates = sorted_snapshots[self.min_snapshots_to_keep:]
        
        if not candidates:
            return []
            
        # Calculate expiration threshold
        expiration_ms = (
            datetime.now() - timedelta(days=self.retention_days)
        ).timestamp() * 1000
        
        return [
            snapshot for snapshot in candidates
            if snapshot.timestamp_ms < expiration_ms
        ]
    
    def cleanup_snapshots(self) -> None:
        """Remove expired snapshots."""
        try:
            table = self.get_table()
            expired = self.get_expired_snapshots()
            
            if not expired:
                logger.info("No expired snapshots found")
                return
                
            logger.info(f"Found {len(expired)} expired snapshots")
            
            for snapshot in expired:
                try:
                    table.expire_snapshots().\
                        expire_older_than(snapshot.timestamp_ms).\
                        commit()
                    logger.info(
                        f"Expired snapshot {snapshot.snapshot_id} "
                        f"from {datetime.fromtimestamp(snapshot.timestamp_ms/1000)}"
                    )
                except Exception as e:
                    logger.error(f"Error expiring snapshot {snapshot.snapshot_id}: {e}")
            
            # Retain history
            self._record_cleanup_history(expired)
            
        except Exception as e:
            logger.error(f"Error during snapshot cleanup: {e}")
            raise
    
    def _record_cleanup_history(self, expired_snapshots: List[Snapshot]) -> None:
        """Record cleanup history for auditing."""
        history_file = "snapshot_cleanup_history.log"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(history_file, "a") as f:
            for snapshot in expired_snapshots:
                f.write(
                    f"{timestamp} | Expired snapshot {snapshot.snapshot_id} | "
                    f"Created: {datetime.fromtimestamp(snapshot.timestamp_ms/1000)}\n"
                )
    
    def get_snapshot_stats(self) -> dict:
        """Get statistics about current snapshots."""
        snapshots = self.list_snapshots()
        if not snapshots:
            return {"total": 0, "expired": 0, "active": 0}
            
        expired = self.get_expired_snapshots()
        return {
            "total": len(snapshots),
            "expired": len(expired),
            "active": len(snapshots) - len(expired)
        }

def main():
    """Main execution function."""
    try:
        manager = SnapshotManager(
            table_name="nessie.timestream.cleaned_trips",
            retention_days=7,
            min_snapshots_to_keep=3
        )
        
        # Print current stats
        stats = manager.get_snapshot_stats()
        logger.info(f"Current snapshot stats: {stats}")
        
        # Perform cleanup
        manager.cleanup_snapshots()
        
        # Print updated stats
        stats = manager.get_snapshot_stats()
        logger.info(f"Updated snapshot stats: {stats}")
        
    except Exception as e:
        logger.error(f"Snapshot management failed: {e}")
        raise

if __name__ == "__main__":
    main() 