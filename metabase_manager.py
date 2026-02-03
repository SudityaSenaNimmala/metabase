"""
Metabase Dashboard Manager Tool
Automates the process of creating, updating, and migrating dashboards across different databases and collections.
"""

import requests
import json
import os
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class MetabaseConfig:
    """Configuration for Metabase connection"""
    base_url: str
    username: str
    password: str
    session_token: Optional[str] = None


class MetabaseManager:
    """Main class for managing Metabase dashboards, questions, and collections"""
    
    def __init__(self, config: MetabaseConfig):
        self.config = config
        self.base_url = config.base_url.rstrip('/')
        self.session_token = None
        self.headers = {}
        
    def authenticate(self) -> bool:
        """Authenticate with Metabase and get session token"""
        try:
            response = requests.post(
                f"{self.base_url}/api/session",
                json={
                    "username": self.config.username,
                    "password": self.config.password
                }
            )
            response.raise_for_status()
            self.session_token = response.json()["id"]
            self.headers = {"X-Metabase-Session": self.session_token}
            logger.info("✓ Successfully authenticated with Metabase")
            return True
        except Exception as e:
            logger.error(f"✗ Authentication failed: {e}")
            return False
    
    def get_databases(self) -> List[Dict]:
        """Get all databases"""
        try:
            response = requests.get(
                f"{self.base_url}/api/database",
                headers=self.headers
            )
            response.raise_for_status()
            databases = response.json()["data"]
            logger.info(f"✓ Found {len(databases)} databases")
            return databases
        except Exception as e:
            logger.error(f"✗ Failed to get databases: {e}")
            return []
    
    def get_collections(self) -> List[Dict]:
        """Get all collections"""
        try:
            response = requests.get(
                f"{self.base_url}/api/collection",
                headers=self.headers
            )
            response.raise_for_status()
            collections = response.json()
            logger.info(f"✓ Found {len(collections)} collections")
            return collections
        except Exception as e:
            logger.error(f"✗ Failed to get collections: {e}")
            return []
    
    def get_questions(self, database_id: Optional[int] = None) -> List[Dict]:
        """Get all questions/cards, optionally filtered by database"""
        try:
            response = requests.get(
                f"{self.base_url}/api/card",
                headers=self.headers
            )
            response.raise_for_status()
            questions = response.json()
            
            if database_id:
                questions = [q for q in questions if q.get("database_id") == database_id]
            
            logger.info(f"✓ Found {len(questions)} questions")
            return questions
        except Exception as e:
            logger.error(f"✗ Failed to get questions: {e}")
            return []
    
    def get_dashboard(self, dashboard_id: int) -> Optional[Dict]:
        """Get dashboard details including all cards"""
        try:
            response = requests.get(
                f"{self.base_url}/api/dashboard/{dashboard_id}",
                headers=self.headers
            )
            response.raise_for_status()
            dashboard = response.json()
            logger.info(f"✓ Retrieved dashboard: {dashboard['name']}")
            return dashboard
        except Exception as e:
            logger.error(f"✗ Failed to get dashboard {dashboard_id}: {e}")
            return None
    
    def get_all_dashboards(self) -> List[Dict]:
        """Get all dashboards"""
        try:
            response = requests.get(
                f"{self.base_url}/api/dashboard",
                headers=self.headers
            )
            response.raise_for_status()
            dashboards = response.json()
            logger.info(f"✓ Found {len(dashboards)} dashboards")
            return dashboards
        except Exception as e:
            logger.error(f"✗ Failed to get dashboards: {e}")
            return []
    
    def create_dashboard(self, name: str, description: str = "", collection_id: Optional[int] = None) -> Optional[Dict]:
        """Create a new dashboard"""
        try:
            payload = {
                "name": name,
                "description": description
            }
            if collection_id:
                payload["collection_id"] = collection_id
                
            response = requests.post(
                f"{self.base_url}/api/dashboard",
                headers=self.headers,
                json=payload
            )
            response.raise_for_status()
            dashboard = response.json()
            logger.info(f"✓ Created dashboard: {name} (ID: {dashboard['id']})")
            return dashboard
        except Exception as e:
            logger.error(f"✗ Failed to create dashboard: {e}")
            return None
    
    def update_dashboard(self, dashboard_id: int, updates: Dict) -> bool:
        """Update dashboard properties"""
        try:
            response = requests.put(
                f"{self.base_url}/api/dashboard/{dashboard_id}",
                headers=self.headers,
                json=updates
            )
            response.raise_for_status()
            logger.info(f"✓ Updated dashboard {dashboard_id}")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to update dashboard: {e}")
            return False
    
    def add_card_to_dashboard(self, dashboard_id: int, card_id: int,
                             row: int = 0, col: int = 0,
                             size_x: int = 4, size_y: int = 4,
                             parameter_mappings: Optional[List[Dict]] = None,
                             visualization_settings: Optional[Dict] = None,
                             series: Optional[List[int]] = None) -> bool:
        """Add a card (question) to a dashboard"""
        try:
            payload = {
                "cardId": card_id,
                "row": row,
                "col": col,
                "size_x": size_x,
                "size_y": size_y
            }
            if parameter_mappings:
                payload["parameter_mappings"] = parameter_mappings
            if visualization_settings:
                payload["visualization_settings"] = visualization_settings
            if series:
                payload["series"] = series
            response = requests.post(
                f"{self.base_url}/api/dashboard/{dashboard_id}/cards",
                headers=self.headers,
                json=payload
            )
            response.raise_for_status()
            logger.info(f"✓ Added card {card_id} to dashboard {dashboard_id}")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to add card to dashboard: {e}")
            return False

    def _remap_parameter_mappings(self, mappings: List[Dict], new_card_id: int) -> List[Dict]:
        remapped = []
        for mapping in mappings:
            mapping_copy = dict(mapping)
            mapping_copy["card_id"] = new_card_id
            remapped.append(mapping_copy)
        return remapped

    def _remap_series(self, series: List[Any], question_mapping: Dict[int, int]) -> List[int]:
        remapped = []
        for item in series:
            if isinstance(item, dict):
                card_id = item.get("id")
            else:
                card_id = item
            if card_id in question_mapping:
                remapped.append(question_mapping[card_id])
        return remapped
    
    def clone_question(self, question_id: int, new_name: str, 
                      new_database_id: Optional[int] = None,
                      collection_id: Optional[int] = None) -> Optional[Dict]:
        """Clone a question and optionally change its database"""
        try:
            # Get original question
            response = requests.get(
                f"{self.base_url}/api/card/{question_id}",
                headers=self.headers
            )
            response.raise_for_status()
            original_question = response.json()
            
            # Create new question with modifications
            new_question = {
                "name": new_name,
                "dataset_query": original_question["dataset_query"],
                "display": original_question["display"],
                "visualization_settings": original_question["visualization_settings"],
                "description": original_question.get("description", "")
            }
            
            if new_database_id:
                new_question["dataset_query"]["database"] = new_database_id
            
            if collection_id:
                new_question["collection_id"] = collection_id
            
            response = requests.post(
                f"{self.base_url}/api/card",
                headers=self.headers,
                json=new_question
            )
            response.raise_for_status()
            cloned_question = response.json()
            logger.info(f"✓ Cloned question: {new_name} (ID: {cloned_question['id']})")
            return cloned_question
        except Exception as e:
            logger.error(f"✗ Failed to clone question: {e}")
            return None
    
    def clone_dashboard(self, dashboard_id: int, new_name: str,
                       database_mapping: Optional[Dict[int, int]] = None,
                       collection_id: Optional[int] = None) -> Optional[Dict]:
        """
        Clone a dashboard with all its questions
        
        Args:
            dashboard_id: ID of dashboard to clone
            new_name: Name for the new dashboard
            database_mapping: Optional dict mapping old database IDs to new ones
            collection_id: Optional collection ID for the new dashboard
        """
        try:
            # Get original dashboard
            original_dashboard = self.get_dashboard(dashboard_id)
            if not original_dashboard:
                return None
            
            # Create new dashboard
            new_dashboard = self.create_dashboard(
                name=new_name,
                description=original_dashboard.get("description", ""),
                collection_id=collection_id
            )
            if not new_dashboard:
                return None
            
            # Preserve dashboard parameters (filters)
            dashboard_parameters = original_dashboard.get("parameters") or []
            if dashboard_parameters:
                self.update_dashboard(new_dashboard["id"], {"parameters": dashboard_parameters})

            # Clone all questions first
            question_mapping = {}  # Map old question IDs to new ones
            ordered_cards = original_dashboard.get("ordered_cards", [])

            for card in ordered_cards:
                original_card = card["card"]
                original_question_id = original_card["id"]

                if original_question_id in question_mapping:
                    continue

                # Determine new database ID if mapping provided
                new_database_id = None
                if database_mapping and original_card.get("database_id"):
                    new_database_id = database_mapping.get(original_card["database_id"])

                # Clone the question
                cloned_question = self.clone_question(
                    question_id=original_question_id,
                    new_name=f"{original_card['name']} (Copy)",
                    new_database_id=new_database_id,
                    collection_id=collection_id
                )

                if cloned_question:
                    question_mapping[original_question_id] = cloned_question["id"]

            # Add cards to dashboard with original layout, filters, and series
            for card in ordered_cards:
                original_card = card["card"]
                original_question_id = original_card["id"]
                new_question_id = question_mapping.get(original_question_id)
                if not new_question_id:
                    continue

                parameter_mappings = card.get("parameter_mappings") or []
                if parameter_mappings:
                    parameter_mappings = self._remap_parameter_mappings(parameter_mappings, new_question_id)

                series = card.get("series") or []
                if series:
                    series = self._remap_series(series, question_mapping)

                self.add_card_to_dashboard(
                    dashboard_id=new_dashboard["id"],
                    card_id=new_question_id,
                    row=card.get("row", 0),
                    col=card.get("col", 0),
                    size_x=card.get("size_x", 4),
                    size_y=card.get("size_y", 4),
                    parameter_mappings=parameter_mappings or None,
                    visualization_settings=card.get("visualization_settings"),
                    series=series or None
                )
            
            logger.info(f"✓ Successfully cloned dashboard with {len(question_mapping)} questions")
            return new_dashboard
        except Exception as e:
            logger.error(f"✗ Failed to clone dashboard: {e}")
            return None
    
    def export_dashboard(self, dashboard_id: int, output_file: str) -> bool:
        """Export dashboard configuration to JSON file"""
        try:
            dashboard = self.get_dashboard(dashboard_id)
            if not dashboard:
                return False
            
            with open(output_file, 'w') as f:
                json.dump(dashboard, f, indent=2)
            
            logger.info(f"✓ Exported dashboard to {output_file}")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to export dashboard: {e}")
            return False
    
    def search_and_replace_database(self, old_database_id: int, new_database_id: int,
                                   dashboard_ids: Optional[List[int]] = None) -> int:
        """
        Update all questions in specified dashboards to use a new database
        
        Returns: Number of questions updated
        """
        updated_count = 0
        dashboards_to_update = dashboard_ids or [d["id"] for d in self.get_all_dashboards()]
        
        for dashboard_id in dashboards_to_update:
            dashboard = self.get_dashboard(dashboard_id)
            if not dashboard:
                continue
            
            for card in dashboard.get("ordered_cards", []):
                question_id = card["card"]["id"]
                
                try:
                    # Get question
                    response = requests.get(
                        f"{self.base_url}/api/card/{question_id}",
                        headers=self.headers
                    )
                    response.raise_for_status()
                    question = response.json()
                    
                    # Check if it uses the old database
                    if question.get("database_id") == old_database_id:
                        question["database_id"] = new_database_id
                        question["dataset_query"]["database"] = new_database_id
                        
                        # Update question
                        response = requests.put(
                            f"{self.base_url}/api/card/{question_id}",
                            headers=self.headers,
                            json=question
                        )
                        response.raise_for_status()
                        updated_count += 1
                        logger.info(f"✓ Updated question {question_id} to use database {new_database_id}")
                
                except Exception as e:
                    logger.error(f"✗ Failed to update question {question_id}: {e}")
        
        logger.info(f"✓ Updated {updated_count} questions to use new database")
        return updated_count


def load_config(config_file: str = "metabase_config.json") -> Optional[MetabaseConfig]:
    """Load configuration from JSON file"""
    try:
        with open(config_file, 'r') as f:
            config_data = json.load(f)
        return MetabaseConfig(**config_data)
    except FileNotFoundError:
        logger.warning(f"Config file {config_file} not found. Using environment variables.")
        return MetabaseConfig(
            base_url=os.getenv("METABASE_URL", "http://localhost:3000"),
            username=os.getenv("METABASE_USERNAME", ""),
            password=os.getenv("METABASE_PASSWORD", "")
        )
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return None


if __name__ == "__main__":
    # Example usage
    config = load_config()
    if not config:
        logger.error("Failed to load configuration")
        exit(1)
    
    manager = MetabaseManager(config)
    if manager.authenticate():
        # List databases
        databases = manager.get_databases()
        for db in databases:
            print(f"Database: {db['name']} (ID: {db['id']})")
        
        # List collections
        collections = manager.get_collections()
        for col in collections:
            print(f"Collection: {col['name']} (ID: {col['id']})")

