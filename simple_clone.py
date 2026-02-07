"""
Dashboard Clone with Database Change
Clones dashboard with all questions, filters, click behaviors, and dashboard links
"""

import sys
import json
import logging
import requests
from difflib import get_close_matches
from typing import Dict, List, Optional, Any
from metabase_manager import MetabaseManager, MetabaseConfig

# Fix Windows console encoding
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except:
        pass

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_config():
    """Load configuration"""
    try:
        with open("metabase_config.json", 'r') as f:
            return json.load(f)
    except:
        return None


class DashboardCloner:
    """Clone dashboards with proper database/table/field mapping and click behavior"""
    
    def __init__(self, config: dict):
        self.config = config
        self.base_url = config['base_url'].rstrip('/')
        self.manager = MetabaseManager(MetabaseConfig(
            base_url=config['base_url'],
            username=config['username'],
            password=config['password']
        ))
        self.headers = {}
        self.table_mapping = {}  # old_table_id -> new_table_id
        self.field_mapping = {}  # old_field_id -> new_field_id
        self.question_mapping = {}  # old_question_id -> new_question_id
        self.dashboard_mapping = {}  # old_dashboard_id -> new_dashboard_id
        self.tab_mapping = {}  # old_tab_id -> new_tab_id (legacy, for single dashboard)
        self.dashboard_tab_mappings = {}  # new_dashboard_id -> {old_tab_id -> new_tab_id}
        
    def authenticate(self) -> bool:
        """Authenticate with Metabase"""
        if self.manager.authenticate():
            self.headers = self.manager.headers
            return True
        return False
    
    def get_databases(self):
        """Get all databases"""
        return self.manager.get_databases()
    
    def get_collections(self):
        """Get all collections"""
        return self.manager.get_collections()
    
    def find_database(self, name: str):
        """Find database by name with fuzzy matching"""
        databases = self.get_databases()
        db_names = [db['name'] for db in databases]
        
        # Exact match (case-insensitive)
        for db in databases:
            if db['name'].lower() == name.lower():
                return db, None
        
        # Fuzzy match
        close_matches = get_close_matches(name, db_names, n=5, cutoff=0.4)
        return None, close_matches
    
    def find_collection(self, name: str):
        """Find collection by name"""
        collections = self.get_collections()
        for col in collections:
            if col['name'].lower() == name.lower():
                return col
        return None
    
    def create_collection(self, name: str, parent_id: int = None) -> dict:
        """Create a new collection, optionally inside a parent collection"""
        try:
            payload = {"name": name}
            if parent_id:
                payload["parent_id"] = parent_id
            
            response = requests.post(
                f"{self.base_url}/api/collection",
                headers=self.headers,
                json=payload
            )
            response.raise_for_status()
            created = response.json()
            logger.info(f"Created collection: {name} (ID: {created['id']})")
            return created
        except Exception as e:
            logger.error(f"Failed to create collection: {e}")
            return None
    
    def get_or_create_collection(self, name: str, parent_id: int = None) -> dict:
        """Find collection by name, or create it if it doesn't exist"""
        existing = self.find_collection(name)
        if existing:
            return existing
        return self.create_collection(name, parent_id)
    
    def get_dashboard_collection_id(self, dashboard_id: int) -> int:
        """Get the collection ID where a dashboard is stored"""
        dashboard = self.manager.get_dashboard(dashboard_id)
        if dashboard:
            return dashboard.get('collection_id')
        return None
    
    def get_database_schema(self, database_id: int) -> dict:
        """Get database schema (tables and fields)"""
        try:
            response = requests.get(
                f"{self.base_url}/api/database/{database_id}/metadata",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get database schema: {e}")
            return {}
    
    def get_question(self, question_id: int) -> dict:
        """Get a question/card by ID"""
        try:
            response = requests.get(
                f"{self.base_url}/api/card/{question_id}",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get question {question_id}: {e}")
            return None
    
    def build_table_field_mapping(self, source_db_id: int, target_db_id: int):
        """Build mapping between source and target database tables/fields by name"""
        logger.info("Building table and field mappings...")
        
        source_schema = self.get_database_schema(source_db_id)
        target_schema = self.get_database_schema(target_db_id)
        
        if not source_schema or not target_schema:
            logger.warning("Could not build schema mappings")
            return
        
        # Build target table lookup by name (case-insensitive, also try without schema prefix)
        target_tables = {}
        target_fields = {}
        target_fields_by_name = {}  # Just field name without table
        
        for table in target_schema.get('tables', []):
            table_name = table['name'].lower()
            # Also store without schema prefix if present
            simple_name = table_name.split('.')[-1] if '.' in table_name else table_name
            
            target_tables[table_name] = table['id']
            target_tables[simple_name] = table['id']
            
            for field in table.get('fields', []):
                field_name = field['name'].lower()
                field_key = f"{table_name}.{field_name}"
                simple_key = f"{simple_name}.{field_name}"
                
                target_fields[field_key] = field['id']
                target_fields[simple_key] = field['id']
                
                # Also store by just field name (for loose matching)
                if field_name not in target_fields_by_name:
                    target_fields_by_name[field_name] = field['id']
        
        # Map source tables/fields to target
        unmapped_tables = []
        unmapped_fields = []
        
        for table in source_schema.get('tables', []):
            table_name = table['name'].lower()
            simple_name = table_name.split('.')[-1] if '.' in table_name else table_name
            
            # Try to find matching target table
            target_id = target_tables.get(table_name) or target_tables.get(simple_name)
            
            if target_id:
                self.table_mapping[table['id']] = target_id
                
                for field in table.get('fields', []):
                    field_name = field['name'].lower()
                    field_key = f"{table_name}.{field_name}"
                    simple_key = f"{simple_name}.{field_name}"
                    
                    # Try different matching strategies
                    target_field_id = (
                        target_fields.get(field_key) or 
                        target_fields.get(simple_key) or
                        target_fields_by_name.get(field_name)  # Fallback: just field name
                    )
                    
                    if target_field_id:
                        self.field_mapping[field['id']] = target_field_id
                    else:
                        unmapped_fields.append(f"{table_name}.{field_name}")
            else:
                unmapped_tables.append(table_name)
        
        logger.info(f"Mapped {len(self.table_mapping)} tables, {len(self.field_mapping)} fields")
        
        if unmapped_tables and len(unmapped_tables) <= 10:
            logger.debug(f"Unmapped tables: {unmapped_tables[:10]}")
        if unmapped_fields and len(unmapped_fields) <= 10:
            logger.debug(f"Unmapped fields: {unmapped_fields[:10]}")
    
    def remap_query(self, query: dict, new_database_id: int) -> dict:
        """Remap a query to use new database with proper table/field IDs"""
        if not query:
            return query
        
        query = json.loads(json.dumps(query))  # Deep copy
        
        # Update database
        if 'database' in query:
            query['database'] = new_database_id
        
        # Handle MBQL queries
        if query.get('type') == 'query' and 'query' in query:
            mbql = query['query']
            
            # Remap source-table
            if 'source-table' in mbql:
                old_table = mbql['source-table']
                if isinstance(old_table, int) and old_table in self.table_mapping:
                    mbql['source-table'] = self.table_mapping[old_table]
            
            # Remap fields recursively
            self._remap_fields_recursive(mbql)
        
        return query
    
    def _remap_fields_recursive(self, obj):
        """Recursively remap field references in any object"""
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, (dict, list)):
                    self._remap_fields_recursive(value)
        elif isinstance(obj, list):
            # Check for field reference ["field", id, options] or ["field", "name", options]
            if len(obj) >= 2 and obj[0] == "field":
                field_ref = obj[1]
                if isinstance(field_ref, int):
                    if field_ref in self.field_mapping:
                        obj[1] = self.field_mapping[field_ref]
                    # else: keep original - might be a field name reference
                elif isinstance(field_ref, str):
                    # Field referenced by name - this usually works across databases
                    pass
                
                # Check source-field in options
                if len(obj) > 2 and isinstance(obj[2], dict):
                    if 'source-field' in obj[2]:
                        src = obj[2]['source-field']
                        if isinstance(src, int) and src in self.field_mapping:
                            obj[2]['source-field'] = self.field_mapping[src]
                    # Also check base-type and other options
            else:
                for item in obj:
                    if isinstance(item, (dict, list)):
                        self._remap_fields_recursive(item)
    
    def remap_click_behavior(self, viz_settings: dict, tab_mapping: dict = None) -> dict:
        """Remap click behavior to point to new dashboards/questions/tabs"""
        if not viz_settings:
            return viz_settings
        
        viz_settings = json.loads(json.dumps(viz_settings))  # Deep copy
        
        # Handle top-level click_behavior (only once!)
        if 'click_behavior' in viz_settings:
            self._remap_single_click_behavior(viz_settings['click_behavior'], tab_mapping)
        
        # Handle column-specific click behaviors (in column_settings)
        if 'column_settings' in viz_settings:
            for col_key, col_settings in viz_settings['column_settings'].items():
                if 'click_behavior' in col_settings:
                    self._remap_single_click_behavior(col_settings['click_behavior'], tab_mapping)
        
        # Handle 'click' key (different from 'click_behavior') for graph.dimensions, etc.
        # NOTE: Don't process 'click_behavior' again - it was already handled above!
        if 'click' in viz_settings and isinstance(viz_settings['click'], dict):
            self._remap_single_click_behavior(viz_settings['click'], tab_mapping)
        
        return viz_settings
    
    def _remap_single_click_behavior(self, click_behavior: dict, tab_mapping: dict = None):
        """Remap a single click behavior object"""
        if not click_behavior or not isinstance(click_behavior, dict):
            return
        
        link_type = click_behavior.get('linkType')
        target_id = click_behavior.get('targetId')
        
        logger.debug(f"    Processing click_behavior: linkType={link_type}, targetId={target_id}, tabId={click_behavior.get('tabId')}")
        
        if link_type == 'dashboard' and target_id:
            # Remap to new dashboard (including self-references)
            new_target = target_id
            if target_id in self.dashboard_mapping:
                new_target = self.dashboard_mapping[target_id]
                click_behavior['targetId'] = new_target
                if target_id == new_target:
                    logger.debug(f"    Dashboard link unchanged: {target_id}")
                else:
                    logger.info(f"    Remapped dashboard link: {target_id} -> {new_target}")
            
            # Remap tab ID if present (dashboard tab navigation)
            if 'tabId' in click_behavior:
                old_tab_id = click_behavior['tabId']
                
                logger.info(f"    Tab remapping: old_tab_id={old_tab_id}, new_target={new_target}")
                logger.info(f"    Available dashboard_tab_mappings: {self.dashboard_tab_mappings}")
                
                # First, try to get the tab mapping for the TARGET dashboard
                # This is important when a dashboard links to itself or another cloned dashboard
                effective_tab_mapping = None
                
                # Check if we have a specific tab mapping for the target dashboard
                if new_target in self.dashboard_tab_mappings:
                    effective_tab_mapping = self.dashboard_tab_mappings[new_target]
                    logger.info(f"    Found tab mapping for target dashboard {new_target}: {effective_tab_mapping}")
                elif tab_mapping:
                    # Fall back to provided tab_mapping (for backward compatibility)
                    effective_tab_mapping = tab_mapping
                    logger.info(f"    Using fallback tab_mapping: {effective_tab_mapping}")
                
                if effective_tab_mapping:
                    # Check if tabId needs remapping (is it a key in the mapping?)
                    if old_tab_id in effective_tab_mapping:
                        click_behavior['tabId'] = effective_tab_mapping[old_tab_id]
                        logger.info(f"    Remapped tab link: {old_tab_id} -> {effective_tab_mapping[old_tab_id]}")
                    elif old_tab_id in effective_tab_mapping.values():
                        # tabId is already a NEW tab ID (already remapped) - don't touch it!
                        logger.info(f"    Tab {old_tab_id} is already a new tab ID - skipping remap")
                    else:
                        # Tab ID not in mapping as key or value - it's invalid, clear it
                        logger.warning(f"    Tab {old_tab_id} not found in mapping for dashboard {new_target} - clearing tabId")
                        del click_behavior['tabId']
                else:
                    logger.warning(f"    No tab mapping available for dashboard {new_target} - keeping original tabId {old_tab_id}")
        
        elif link_type == 'question' and target_id:
            # Remap to new question
            if target_id in self.question_mapping:
                click_behavior['targetId'] = self.question_mapping[target_id]
                logger.info(f"    Remapped question link: {target_id} -> {self.question_mapping[target_id]}")
        
        # Remap parameter mappings within click behavior
        if 'parameterMapping' in click_behavior:
            param_mapping = click_behavior['parameterMapping']
            for param_id, mapping in param_mapping.items():
                if 'source' in mapping and isinstance(mapping['source'], dict):
                    source = mapping['source']
                    if source.get('type') == 'column':
                        # Remap field ID if present
                        if 'id' in source and isinstance(source['id'], list):
                            self._remap_fields_recursive(source['id'])
    
    def remap_parameter_mappings(self, mappings: list, new_card_id: int) -> list:
        """Remap parameter mappings with new card ID and field IDs"""
        if not mappings:
            return []
        
        remapped = []
        for mapping in mappings:
            mapping_copy = json.loads(json.dumps(mapping))
            mapping_copy['card_id'] = new_card_id
            
            # Remap target field
            target = mapping_copy.get('target', [])
            if target:
                old_target = json.dumps(target)
                self._remap_fields_recursive(target)
                new_target = json.dumps(target)
                if old_target != new_target:
                    logger.debug(f"    Remapped parameter target: {old_target[:50]} -> {new_target[:50]}")
                elif 'field' in old_target:
                    # Field wasn't remapped - might cause issues
                    logger.warning(f"    Could not remap parameter field in: {old_target[:80]}")
            
            remapped.append(mapping_copy)
        return remapped
    
    def remap_dashboard_parameters(self, parameters: list) -> list:
        """
        Remap dashboard parameters (filters) to use cloned question IDs.
        
        Dashboard filters can have a values_source_config that specifies a card_id
        to fetch dropdown values from. This needs to be remapped to the cloned question.
        """
        if not parameters:
            return []
        
        remapped = []
        for param in parameters:
            param_copy = json.loads(json.dumps(param))  # Deep copy
            
            # Check for values_source_config with card_id
            values_source = param_copy.get('values_source_config', {})
            if values_source and 'card_id' in values_source:
                old_card_id = values_source['card_id']
                if old_card_id in self.question_mapping:
                    new_card_id = self.question_mapping[old_card_id]
                    values_source['card_id'] = new_card_id
                    logger.info(f"  Remapped filter '{param_copy.get('name', 'Unknown')}' values_source: card {old_card_id} -> {new_card_id}")
                else:
                    logger.warning(f"  Filter '{param_copy.get('name', 'Unknown')}' references card {old_card_id} which wasn't cloned")
            
            # Also check for values_query_type = "card" which indicates card-based values
            if param_copy.get('values_query_type') == 'card' and values_source:
                # Already handled above, but log for debugging
                logger.debug(f"  Filter '{param_copy.get('name', 'Unknown')}' uses card-based values")
            
            remapped.append(param_copy)
        
        return remapped
    
    def find_filter_linked_questions(self, parameters: list) -> list:
        """
        Find all question IDs referenced by dashboard filter dropdowns.
        
        Dashboard filters can have values_source_config.card_id that specifies
        a question to fetch dropdown values from. These "hidden" questions
        need to be cloned even though they're not visible on the dashboard.
        
        Returns a list of question IDs that need to be cloned.
        """
        if not parameters:
            return []
        
        question_ids = []
        for param in parameters:
            values_source = param.get('values_source_config', {})
            if values_source and 'card_id' in values_source:
                card_id = values_source['card_id']
                if card_id and card_id not in question_ids:
                    question_ids.append(card_id)
                    logger.info(f"  Found filter-linked question: {card_id} (for filter '{param.get('name', 'Unknown')}')")
        
        return question_ids
    
    def clone_filter_linked_questions(self, parameters: list, new_database_id: int, 
                                       collection_id: int = None) -> dict:
        """
        Clone all questions referenced by dashboard filter dropdowns.
        
        These are "hidden" questions that provide dropdown values but aren't
        displayed on any dashboard. They need to be cloned to the new database
        so the filter dropdowns show values from the correct database.
        
        Returns a mapping of old_question_id -> new_question_id
        """
        question_ids = self.find_filter_linked_questions(parameters)
        
        if not question_ids:
            return {}
        
        logger.info(f"\n--- Cloning {len(question_ids)} filter-linked questions ---")
        
        cloned_mapping = {}
        for question_id in question_ids:
            if question_id in self.question_mapping:
                logger.info(f"  Question {question_id} already cloned, skipping")
                continue
            
            # Get the original question to find its name
            original = self.get_question(question_id)
            if not original:
                logger.warning(f"  Could not fetch filter-linked question {question_id}")
                continue
            
            original_name = original.get('name', f'Filter Question {question_id}')
            
            # Clone the question
            cloned = self.clone_question(
                question_id=question_id,
                new_name=original_name,
                new_database_id=new_database_id,
                collection_id=collection_id
            )
            
            if cloned:
                cloned_mapping[question_id] = cloned['id']
                self.question_mapping[question_id] = cloned['id']
                logger.info(f"  + Cloned filter question: {original_name} ({question_id} -> {cloned['id']})")
            else:
                logger.error(f"  x Failed to clone filter question {question_id}")
        
        return cloned_mapping
    
    def clone_question(self, question_id: int, new_name: str, 
                      new_database_id: int, collection_id: int = None,
                      max_retries: int = 3) -> dict:
        """Clone a question with remapped database/tables/fields.
        Includes automatic retry mechanism for intermittent failures."""
        import uuid
        import time
        
        for attempt in range(1, max_retries + 1):
            try:
                # Get original question
                original = self.get_question(question_id)
                if not original:
                    logger.error(f"  x Could not fetch original question {question_id}")
                    return None
                
                # Remap the query
                new_query = self.remap_query(original.get('dataset_query', {}), new_database_id)
                
                # IMPORTANT: Regenerate template tag IDs to avoid conflicts
                # Template tags can be in different locations depending on query format
                self._regenerate_template_tag_ids(new_query)
                
                # Remap visualization settings (including click behavior)
                new_viz_settings = self.remap_click_behavior(original.get('visualization_settings', {}))
                
                # Build new question
                new_question = {
                    "name": new_name,
                    "dataset_query": new_query,
                    "display": original.get('display', 'table'),
                    "visualization_settings": new_viz_settings,
                    "description": original.get('description', '')
                }
                
                if collection_id:
                    new_question['collection_id'] = collection_id
                
                # Create the question
                response = requests.post(
                    f"{self.base_url}/api/card",
                    headers=self.headers,
                    json=new_question
                )
                response.raise_for_status()
                created = response.json()
                logger.info(f"  + Cloned question: {new_name} (ID: {created['id']})")
                return created
            
            except Exception as e:
                if attempt < max_retries:
                    logger.warning(f"  ! Attempt {attempt}/{max_retries} failed for question {question_id}: {e}")
                    logger.info(f"    Retrying in {attempt * 2} seconds...")
                    time.sleep(attempt * 2)  # Exponential backoff: 2s, 4s, 6s
                else:
                    logger.error(f"  x Failed to clone question {question_id} after {max_retries} attempts: {e}")
                    return None
        
        return None
    
    def _regenerate_template_tag_ids(self, query: dict):
        """Regenerate UUIDs for all template tags to avoid conflicts"""
        import uuid
        
        if not query:
            return
        
        # Handle 'native' format (older Metabase)
        if 'native' in query and isinstance(query['native'], dict):
            template_tags = query['native'].get('template-tags', {})
            for tag_name, tag_info in template_tags.items():
                if isinstance(tag_info, dict) and 'id' in tag_info:
                    old_id = tag_info['id']
                    tag_info['id'] = str(uuid.uuid4())
                    logger.debug(f"    Regenerated template tag '{tag_name}' ID: {old_id[:8]}... -> {tag_info['id'][:8]}...")
        
        # Handle 'stages' format (newer Metabase MBQL v2)
        if 'stages' in query and isinstance(query['stages'], list):
            for stage in query['stages']:
                if isinstance(stage, dict):
                    template_tags = stage.get('template-tags', {})
                    for tag_name, tag_info in template_tags.items():
                        if isinstance(tag_info, dict) and 'id' in tag_info:
                            old_id = tag_info['id']
                            tag_info['id'] = str(uuid.uuid4())
                            logger.debug(f"    Regenerated template tag '{tag_name}' ID: {old_id[:8]}... -> {tag_info['id'][:8]}...")
    
    def add_dashcards_with_tabs(self, dashboard_id: int, dashcards: List[dict], 
                                  tabs: List[dict], source_tabs: List[dict]) -> tuple:
        """
        Add cards AND tabs to dashboard in a single atomic request.
        Returns (success, tab_mapping) where tab_mapping is old_tab_id -> new_tab_id
        """
        try:
            # Build dashcards payload
            cards_payload = []
            for i, dc in enumerate(dashcards):
                card_payload = {
                    "id": -(i + 1),  # Negative IDs for new cards
                    "card_id": dc['card_id'],
                    "row": dc.get('row', 0),
                    "col": dc.get('col', 0),
                    "size_x": dc.get('size_x', 4),
                    "size_y": dc.get('size_y', 4),
                    "parameter_mappings": dc.get('parameter_mappings', []),
                    "visualization_settings": dc.get('visualization_settings', {}),
                }
                
                # Add tab ID if present (using the negative ID we pre-mapped)
                if dc.get('dashboard_tab_id'):
                    card_payload['dashboard_tab_id'] = dc['dashboard_tab_id']
                
                # Add series if present
                if dc.get('series'):
                    card_payload['series'] = dc['series']
                
                cards_payload.append(card_payload)
            
            # Build update payload with BOTH tabs and dashcards
            update_payload = {
                "dashcards": cards_payload
            }
            
            if tabs:
                update_payload['tabs'] = tabs
                logger.info(f"  Including {len(tabs)} tabs in update")
            
            logger.info(f"  Updating dashboard with {len(cards_payload)} cards...")
            
            response = requests.put(
                f"{self.base_url}/api/dashboard/{dashboard_id}",
                headers=self.headers,
                json=update_payload
            )
            response.raise_for_status()
            
            # Get the dashboard to retrieve actual tab IDs
            tab_mapping = {}
            if tabs and source_tabs:
                get_response = requests.get(
                    f"{self.base_url}/api/dashboard/{dashboard_id}",
                    headers=self.headers
                )
                get_response.raise_for_status()
                updated_dash = get_response.json()
                created_tabs = updated_dash.get('tabs', [])
                
                if len(created_tabs) == len(source_tabs):
                    for orig_tab, new_tab in zip(source_tabs, created_tabs):
                        tab_mapping[orig_tab['id']] = new_tab['id']
                        logger.info(f"  Tab created: {orig_tab.get('name')} ({orig_tab['id']} -> {new_tab['id']})")
                else:
                    # Match by name as fallback
                    created_by_name = {t.get('name', '').lower(): t['id'] for t in created_tabs}
                    for orig_tab in source_tabs:
                        name = orig_tab.get('name', '').lower()
                        if name in created_by_name:
                            tab_mapping[orig_tab['id']] = created_by_name[name]
                
                logger.info(f"  Created {len(created_tabs)} tabs")
            
            logger.info(f"  + Added {len(cards_payload)} cards via dashboard update")
            return True, tab_mapping
            
        except Exception as e:
            logger.error(f"  x Failed to add cards with tabs: {e}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    logger.error(f"  Response: {e.response.text[:500]}")
                except:
                    pass
            return False, {}
    
    def add_dashcards_via_dashboard_update(self, dashboard_id: int, dashcards: List[dict]) -> bool:
        """
        Add cards to dashboard by updating the dashboard with dashcards.
        Uses PUT /api/dashboard/{id} with dashcards in the payload.
        This works on Metabase versions where POST /cards doesn't exist.
        """
        try:
            # First get current dashboard state
            response = requests.get(
                f"{self.base_url}/api/dashboard/{dashboard_id}",
                headers=self.headers
            )
            response.raise_for_status()
            current = response.json()
            
            # Build dashcards payload - use format expected by PUT /api/dashboard
            cards_payload = []
            for i, dc in enumerate(dashcards):
                card_payload = {
                    "id": -(i + 1),  # Negative IDs for new cards
                    "card_id": dc['card_id'],
                    "row": dc.get('row', 0),
                    "col": dc.get('col', 0),
                    "size_x": dc.get('size_x', 4),
                    "size_y": dc.get('size_y', 4),
                    "parameter_mappings": dc.get('parameter_mappings', []),
                    # Only include click_behavior from visualization_settings
                    # to preserve links without causing filter conflicts
                    "visualization_settings": dc.get('visualization_settings', {}),
                }
                
                # Add tab ID if present
                if dc.get('dashboard_tab_id'):
                    card_payload['dashboard_tab_id'] = dc['dashboard_tab_id']
                
                # Add series if present (for combined charts)
                if dc.get('series'):
                    card_payload['series'] = dc['series']
                
                cards_payload.append(card_payload)
            
            # Update dashboard with dashcards - preserve existing tabs!
            update_payload = {
                "dashcards": cards_payload
            }
            
            # Preserve tabs if they exist
            current_tabs = current.get('tabs', [])
            if current_tabs:
                update_payload['tabs'] = current_tabs
                logger.info(f"  Preserving {len(current_tabs)} tabs during card update")
            
            logger.info(f"  Updating dashboard with {len(cards_payload)} cards...")
            
            response = requests.put(
                f"{self.base_url}/api/dashboard/{dashboard_id}",
                headers=self.headers,
                json=update_payload
            )
            response.raise_for_status()
            
            logger.info(f"  + Added {len(cards_payload)} cards via dashboard update")
            return True
        
        except Exception as e:
            logger.error(f"  x Failed to add cards via dashboard update: {e}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    logger.error(f"  Response: {e.response.text[:500]}")
                except:
                    pass
            return False
    
    def update_all_dashcards(self, dashboard_id: int, dashcards: List[dict]) -> bool:
        """
        Update all dashcards on a dashboard using PUT /api/dashboard/{id}
        """
        try:
            response = requests.put(
                f"{self.base_url}/api/dashboard/{dashboard_id}",
                headers=self.headers,
                json={"dashcards": dashcards}
            )
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"  x Failed to update dashcards: {e}")
            return False
    
    def update_dashboard_click_behaviors(self, dashboard_id: int, tab_mapping: dict = None) -> bool:
        """
        Update click behaviors in a dashboard with the current mapping.
        Call this after all dashboards are cloned to fix cross-references.
        
        Args:
            dashboard_id: The dashboard to update
            tab_mapping: Optional mapping of old_tab_id -> new_tab_id for tab links
                        (Note: per-dashboard tab mappings in self.dashboard_tab_mappings
                        take precedence for cross-dashboard tab references)
        """
        try:
            # Get current dashboard
            response = requests.get(
                f"{self.base_url}/api/dashboard/{dashboard_id}",
                headers=self.headers
            )
            response.raise_for_status()
            dashboard = response.json()
            
            dashcards = dashboard.get('dashcards', []) or dashboard.get('ordered_cards', [])
            if not dashcards:
                return True
            
            # Update each dashcard's click behaviors
            updated_dashcards = []
            changes_made = False
            
            for dc in dashcards:
                viz_settings = dc.get('visualization_settings', {})
                card_name = dc.get('card', {}).get('name', 'Unknown')
                
                if viz_settings:
                    original = json.dumps(viz_settings, sort_keys=True)
                    logger.debug(f"  Checking dashcard '{card_name}' viz_settings before remap")
                    
                    # Log click behaviors before remapping
                    if 'click_behavior' in viz_settings:
                        logger.info(f"    Before remap - click_behavior: targetId={viz_settings['click_behavior'].get('targetId')}, tabId={viz_settings['click_behavior'].get('tabId', 'NOT SET')}")
                    if 'column_settings' in viz_settings:
                        for col_key, col_settings in viz_settings['column_settings'].items():
                            if 'click_behavior' in col_settings:
                                cb = col_settings['click_behavior']
                                logger.info(f"    Before remap - column click_behavior: targetId={cb.get('targetId')}, tabId={cb.get('tabId', 'NOT SET')}")
                    
                    remapped = self.remap_click_behavior(viz_settings, tab_mapping)
                    
                    # Log click behaviors after remapping
                    if 'click_behavior' in remapped:
                        logger.info(f"    After remap - click_behavior: targetId={remapped['click_behavior'].get('targetId')}, tabId={remapped['click_behavior'].get('tabId', 'NOT SET')}")
                    if 'column_settings' in remapped:
                        for col_key, col_settings in remapped['column_settings'].items():
                            if 'click_behavior' in col_settings:
                                cb = col_settings['click_behavior']
                                logger.info(f"    After remap - column click_behavior: targetId={cb.get('targetId')}, tabId={cb.get('tabId', 'NOT SET')}")
                    
                    remapped_str = json.dumps(remapped, sort_keys=True)
                    if remapped_str != original:
                        changes_made = True
                        dc['visualization_settings'] = remapped
                        logger.info(f"    Changes detected for '{card_name}'")
                
                dashcard_update = {
                    'id': dc['id'],
                    'card_id': dc.get('card_id'),
                    'row': dc.get('row', 0),
                    'col': dc.get('col', 0),
                    'size_x': dc.get('size_x', 4),
                    'size_y': dc.get('size_y', 4),
                    'parameter_mappings': dc.get('parameter_mappings', []),
                    'visualization_settings': dc.get('visualization_settings', {}),
                    'series': dc.get('series', [])
                }
                # Preserve tab assignment
                if dc.get('dashboard_tab_id'):
                    dashcard_update['dashboard_tab_id'] = dc['dashboard_tab_id']
                updated_dashcards.append(dashcard_update)
            
            if not changes_made:
                logger.debug(f"    No click behavior changes needed for dashboard {dashboard_id}")
                return True
            
            # Update dashboard with fixed click behaviors - preserve tabs!
            update_payload = {'dashcards': updated_dashcards}
            current_tabs = dashboard.get('tabs', [])
            if current_tabs:
                update_payload['tabs'] = current_tabs
            
            response = requests.put(
                f"{self.base_url}/api/dashboard/{dashboard_id}",
                headers=self.headers,
                json=update_payload
            )
            response.raise_for_status()
            return True
            
        except Exception as e:
            logger.error(f"  Failed to update click behaviors: {e}")
            return False
    
    def analyze_dashboard_links(self, dashboard_id: int) -> set:
        """Find all dashboards that this dashboard links to"""
        linked_dashboards = set()
        
        dashboard = self.manager.get_dashboard(dashboard_id)
        if not dashboard:
            return linked_dashboards
        
        # Note: Metabase API may return 'dashcards' or 'ordered_cards'
        dashcards = dashboard.get('dashcards', []) or dashboard.get('ordered_cards', [])
        for dashcard in dashcards:
            viz_settings = dashcard.get('visualization_settings', {})
            
            # Check click_behavior
            if 'click_behavior' in viz_settings:
                cb = viz_settings['click_behavior']
                if cb.get('linkType') == 'dashboard' and cb.get('targetId'):
                    linked_dashboards.add(cb['targetId'])
            
            # Check column settings
            if 'column_settings' in viz_settings:
                for col_settings in viz_settings['column_settings'].values():
                    if 'click_behavior' in col_settings:
                        cb = col_settings['click_behavior']
                        if cb.get('linkType') == 'dashboard' and cb.get('targetId'):
                            linked_dashboards.add(cb['targetId'])
            
            # Also check the card's visualization settings
            card_info = dashcard.get('card', {})
            card_viz = card_info.get('visualization_settings', {})
            if 'click_behavior' in card_viz:
                cb = card_viz['click_behavior']
                if cb.get('linkType') == 'dashboard' and cb.get('targetId'):
                    linked_dashboards.add(cb['targetId'])
        
        return linked_dashboards
    
    def diagnose_click_behaviors(self, dashboard_id: int):
        """
        Diagnostic function to print all click behaviors in a dashboard.
        Use this to debug tab navigation issues.
        """
        dashboard = self.manager.get_dashboard(dashboard_id)
        if not dashboard:
            logger.error(f"Dashboard {dashboard_id} not found")
            return
        
        logger.info(f"\n{'='*60}")
        logger.info(f"CLICK BEHAVIOR DIAGNOSIS FOR DASHBOARD {dashboard_id}")
        logger.info(f"Dashboard name: {dashboard.get('name')}")
        logger.info(f"{'='*60}")
        
        # Show tabs
        tabs = dashboard.get('tabs', [])
        if tabs:
            logger.info(f"\nTabs ({len(tabs)}):")
            for tab in tabs:
                logger.info(f"  - ID: {tab.get('id')}, Name: {tab.get('name')}")
        else:
            logger.info("\nNo tabs in this dashboard")
        
        # Show click behaviors
        dashcards = dashboard.get('dashcards', []) or dashboard.get('ordered_cards', [])
        logger.info(f"\nDashcards ({len(dashcards)}):")
        
        for i, dashcard in enumerate(dashcards):
            card_info = dashcard.get('card', {})
            card_name = card_info.get('name', 'Unknown')
            dashcard_tab = dashcard.get('dashboard_tab_id')
            
            logger.info(f"\n  [{i+1}] Card: {card_name}")
            logger.info(f"      Dashcard ID: {dashcard.get('id')}")
            logger.info(f"      On Tab ID: {dashcard_tab}")
            
            viz_settings = dashcard.get('visualization_settings', {})
            
            # Top-level click behavior
            if 'click_behavior' in viz_settings:
                cb = viz_settings['click_behavior']
                logger.info(f"      Top-level click_behavior:")
                logger.info(f"        linkType: {cb.get('linkType')}")
                logger.info(f"        targetId: {cb.get('targetId')}")
                logger.info(f"        tabId: {cb.get('tabId', 'NOT SET')}")
                if 'parameterMapping' in cb:
                    logger.info(f"        parameterMapping: {list(cb['parameterMapping'].keys())}")
            
            # Column settings click behaviors
            if 'column_settings' in viz_settings:
                for col_key, col_settings in viz_settings['column_settings'].items():
                    if 'click_behavior' in col_settings:
                        cb = col_settings['click_behavior']
                        logger.info(f"      Column click_behavior [{col_key[:30]}...]:")
                        logger.info(f"        linkType: {cb.get('linkType')}")
                        logger.info(f"        targetId: {cb.get('targetId')}")
                        logger.info(f"        tabId: {cb.get('tabId', 'NOT SET')}")
                        if 'parameterMapping' in cb:
                            logger.info(f"        parameterMapping: {list(cb['parameterMapping'].keys())}")
        
        logger.info(f"\n{'='*60}\n")
    
    def clone_dashboard(self, source_dashboard_id: int, new_name: str,
                       new_database_id: int, dashboard_collection_id: int = None,
                       questions_collection_id: int = None,
                       dashboard_links_mapping: Dict[int, int] = None) -> dict:
        """
        Clone a dashboard with all questions remapped to new database
        
        Args:
            source_dashboard_id: ID of source dashboard
            new_name: Name for new dashboard
            new_database_id: Target database ID
            dashboard_collection_id: Collection for the new dashboard
            questions_collection_id: Collection for the cloned questions
            dashboard_links_mapping: Mapping of old dashboard IDs to new ones for click behavior
        """
        # Store dashboard link mappings
        if dashboard_links_mapping:
            self.dashboard_mapping.update(dashboard_links_mapping)
        
        # Get source dashboard
        logger.info(f"Getting source dashboard {source_dashboard_id}...")
        source = self.manager.get_dashboard(source_dashboard_id)
        if not source:
            raise Exception(f"Dashboard {source_dashboard_id} not found")
        
        logger.info(f"Source dashboard: {source.get('name')}")
        
        # Find source database from questions
        # Note: Metabase API may return 'dashcards' or 'ordered_cards'
        source_db_id = None
        ordered_cards = source.get('dashcards', []) or source.get('ordered_cards', [])
        
        for dashcard in ordered_cards:
            card_info = dashcard.get('card', {})
            if card_info and card_info.get('id'):
                db_id = card_info.get('database_id')
                if db_id:
                    source_db_id = db_id
            break
    
        if source_db_id:
            logger.info(f"Source database ID: {source_db_id}, Target database ID: {new_database_id}")
            self.build_table_field_mapping(source_db_id, new_database_id)
        else:
            logger.warning("Could not determine source database - will try without field mapping")
        
        # Analyze linked dashboards
        linked_dashboards = self.analyze_dashboard_links(source_dashboard_id)
        if linked_dashboards:
            logger.info(f"Dashboard links to: {linked_dashboards}")
            unmapped = linked_dashboards - set(self.dashboard_mapping.keys())
            if unmapped:
                logger.warning(f"Warning: No mapping for linked dashboards: {unmapped}")
                logger.warning("Click behaviors to these dashboards will keep original IDs")
        
        # Create new dashboard
        logger.info(f"Creating new dashboard: {new_name}...")
        new_dashboard = self.manager.create_dashboard(
            name=new_name,
            description=source.get('description', ''),
            collection_id=dashboard_collection_id
        )
        if not new_dashboard:
            raise Exception("Failed to create dashboard")
        
        logger.info(f"Created dashboard ID: {new_dashboard['id']}")
        
        # IMPORTANT: Store the dashboard mapping NOW so self-references can be remapped
        # This allows click behaviors that link to the same dashboard to be properly updated
        self.dashboard_mapping[source_dashboard_id] = new_dashboard['id']
        
        # Store dashboard parameters (filters) - will be remapped AFTER questions are cloned
        # because filter dropdowns may reference questions for their values
        dashboard_params = source.get('parameters', [])
        if dashboard_params:
            logger.info(f"Found {len(dashboard_params)} dashboard filters")
            
            # IMPORTANT: Clone filter-linked questions FIRST
            # These are "hidden" questions that provide dropdown values but aren't on the dashboard
            # They need to be cloned before we can remap the filter parameters
            self.clone_filter_linked_questions(
                parameters=dashboard_params,
                new_database_id=new_database_id,
                collection_id=questions_collection_id
            )
        
        # Prepare tabs for later - will be created together with dashcards
        tab_mapping = {}  # old_tab_id -> new_tab_id  
        source_tabs = source.get('tabs', [])
        new_tabs_for_update = []
        if source_tabs:
            logger.info(f"Dashboard has {len(source_tabs)} tabs - will create with cards")
            # Prepare tabs with negative IDs
            for i, tab in enumerate(source_tabs):
                new_tab = {
                    'id': -(i + 1),  # Negative IDs for new tabs
                    'name': tab.get('name', f'Tab {i+1}'),
                    'position': tab.get('position', i)
                }
                new_tabs_for_update.append(new_tab)
                # Pre-map: old tab ID -> negative ID (will be updated after creation)
                tab_mapping[tab['id']] = -(i + 1)
        
        # Phase 1: Clone all questions first (to build question_mapping)
        logger.info(f"\n--- Cloning {len(ordered_cards)} cards ---")
        
        for dashcard in ordered_cards:
            card_info = dashcard.get('card', {})
            original_id = card_info.get('id')
            
            if not original_id:
                # This might be a text card or virtual card
                logger.info(f"  Skipping card without ID (text/virtual card)")
                continue
            
            if original_id in self.question_mapping:
                continue  # Already cloned
            
            original_name = card_info.get('name', f'Question {original_id}')
            
            # Clone the question
            cloned = self.clone_question(
                question_id=original_id,
                new_name=original_name,
                new_database_id=new_database_id,
                collection_id=questions_collection_id
            )
            
            if cloned:
                self.question_mapping[original_id] = cloned['id']
        
        logger.info(f"\nCloned {len(self.question_mapping)} questions")
        
        # Now remap and apply dashboard parameters (filters)
        # This must happen AFTER questions are cloned so we can remap card references
        if dashboard_params:
            remapped_params = self.remap_dashboard_parameters(dashboard_params)
            self.manager.update_dashboard(new_dashboard['id'], {'parameters': remapped_params})
            logger.info(f"Applied {len(remapped_params)} remapped dashboard filters")
        
        # Phase 2: Prepare all cards for batch add
        logger.info(f"\n--- Preparing cards for dashboard ---")
        dashcards_to_add = []
        
        for dashcard in ordered_cards:
            card_info = dashcard.get('card', {})
            original_id = card_info.get('id')
            
            # Handle text/virtual cards (no card_id)
            if not original_id:
                # Text cards don't have a card_id - just copy their layout and settings
                viz_settings = dashcard.get('visualization_settings', {})
                
                text_card_data = {
                    'card_id': None,  # Text cards have null card_id
                    'row': dashcard.get('row', 0),
                    'col': dashcard.get('col', 0),
                    'size_x': dashcard.get('size_x', 4),
                    'size_y': dashcard.get('size_y', 2),
                    'parameter_mappings': [],
                    'visualization_settings': viz_settings,  # Contains the text content
                    'series': []
                }
                
                # Map tab ID if on a tab
                old_tab_id = dashcard.get('dashboard_tab_id')
                if old_tab_id and old_tab_id in tab_mapping:
                    text_card_data['dashboard_tab_id'] = tab_mapping[old_tab_id]
                elif old_tab_id:
                    text_card_data['dashboard_tab_id'] = old_tab_id
                
                dashcards_to_add.append(text_card_data)
                logger.info(f"  Prepared text/virtual card")
                continue
            
            new_card_id = self.question_mapping.get(original_id)
            if not new_card_id:
                logger.warning(f"  Skipping card {original_id} - no cloned version")
                continue
            
            # Remap parameter mappings
            param_mappings = dashcard.get('parameter_mappings', [])
            if param_mappings:
                param_mappings = self.remap_parameter_mappings(param_mappings, new_card_id)
            
            # Remap visualization settings (click behavior)
            viz_settings = dashcard.get('visualization_settings', {})
            if viz_settings:
                # Log original click behaviors for debugging
                if 'click_behavior' in viz_settings:
                    logger.info(f"  Original click_behavior: {json.dumps(viz_settings['click_behavior'], indent=2)}")
                if 'column_settings' in viz_settings:
                    for col_key, col_settings in viz_settings['column_settings'].items():
                        if 'click_behavior' in col_settings:
                            logger.info(f"  Original column click_behavior [{col_key}]: {json.dumps(col_settings['click_behavior'], indent=2)}")
                viz_settings = self.remap_click_behavior(viz_settings)
            
            # Remap series (for combined charts)
            # Series can be a list of card IDs or card objects
            series = dashcard.get('series', [])
            remapped_series = []
            if series:
                for s in series:
                    if isinstance(s, dict):
                        old_id = s.get('id')
                        if old_id in self.question_mapping:
                            # Keep the full object structure, just update the ID
                            new_series_item = json.loads(json.dumps(s))
                            new_series_item['id'] = self.question_mapping[old_id]
                            remapped_series.append(new_series_item)
                        else:
                            logger.warning(f"  Could not remap series card {old_id}")
                    elif isinstance(s, int):
                        if s in self.question_mapping:
                            remapped_series.append(self.question_mapping[s])
                        else:
                            logger.warning(f"  Could not remap series card {s}")
            
            # Prepare card data
            # Keep click_behavior (top-level and in column_settings) to preserve links
            # Avoid copying other viz settings that could conflict with filters
            clean_viz_settings = {}
            if viz_settings:
                # Top-level click behavior
                if 'click_behavior' in viz_settings:
                    clean_viz_settings['click_behavior'] = viz_settings['click_behavior']
                
                # Column-level click behaviors (very common for tables/charts)
                if 'column_settings' in viz_settings:
                    clean_column_settings = {}
                    for col_key, col_settings in viz_settings['column_settings'].items():
                        if 'click_behavior' in col_settings:
                            clean_column_settings[col_key] = {'click_behavior': col_settings['click_behavior']}
                    if clean_column_settings:
                        clean_viz_settings['column_settings'] = clean_column_settings
            
            dashcard_data = {
                'card_id': new_card_id,
                'row': dashcard.get('row', 0),
                'col': dashcard.get('col', 0),
                'size_x': dashcard.get('size_x', 4),
                'size_y': dashcard.get('size_y', 4),
                'parameter_mappings': param_mappings if param_mappings else [],
                'visualization_settings': clean_viz_settings,
                'series': remapped_series if remapped_series else []
            }
            
            # Map tab ID if dashcard is on a tab
            old_tab_id = dashcard.get('dashboard_tab_id')
            if old_tab_id and old_tab_id in tab_mapping:
                dashcard_data['dashboard_tab_id'] = tab_mapping[old_tab_id]
            elif old_tab_id:
                # Keep original if no mapping (shouldn't happen)
                dashcard_data['dashboard_tab_id'] = old_tab_id
            
            dashcards_to_add.append(dashcard_data)
            logger.info(f"  Prepared card: {card_info.get('name', 'Unknown')}")
        
        # Add all cards via dashboard update (works on all Metabase versions)
        # Include tabs in the same request to create them atomically
        logger.info(f"\n--- Adding {len(dashcards_to_add)} cards to dashboard ---")
        if dashcards_to_add or new_tabs_for_update:
            success, actual_tab_mapping = self.add_dashcards_with_tabs(
                new_dashboard['id'], 
                dashcards_to_add, 
                new_tabs_for_update,
                source_tabs
            )
            
            # Update tab_mapping with actual IDs
            if actual_tab_mapping:
                tab_mapping = actual_tab_mapping
                # Store tab mapping for use in click behavior updates
                self.tab_mapping = actual_tab_mapping
                # Also store per-dashboard tab mapping for cross-dashboard tab references
                self.dashboard_tab_mappings[new_dashboard['id']] = actual_tab_mapping
                logger.info(f"  Stored tab mapping for dashboard {new_dashboard['id']}: {actual_tab_mapping}")
            
            if not success:
                logger.error("  Failed to add cards to dashboard!")
            else:
                # Verify cards were added
                updated = self.manager.get_dashboard(new_dashboard['id'])
                if updated:
                    final_cards = updated.get('dashcards', []) or updated.get('ordered_cards', [])
                    final_tabs = updated.get('tabs', [])
                    logger.info(f"  Verified: Dashboard has {len(final_cards)} cards, {len(final_tabs)} tabs")
        
        # Phase 3: Update click behaviors with actual tab IDs
        # This is needed because click behaviors may reference tabs on the target dashboard
        # and we now have the real tab IDs after creation
        if tab_mapping:
            logger.info(f"\n--- Updating click behaviors with tab mappings ---")
            logger.info(f"  tab_mapping: {tab_mapping}")
            logger.info(f"  dashboard_tab_mappings: {self.dashboard_tab_mappings}")
            logger.info(f"  dashboard_mapping: {self.dashboard_mapping}")
            self.update_dashboard_click_behaviors(new_dashboard['id'], tab_mapping)
        
        logger.info(f"\n=== Dashboard cloned successfully! ===")
        
        return new_dashboard
    
    def find_all_linked_dashboards(self, dashboard_id: int, visited: set = None) -> List[int]:
        """
        Find ALL dashboards linked from this dashboard (recursively).
        Returns them in order suitable for cloning (deepest first).
        """
        if visited is None:
            visited = set()
        
        if dashboard_id in visited:
            return []
        
        visited.add(dashboard_id)
        result = []
        
        # Get direct links from this dashboard
        direct_links = self.analyze_dashboard_links(dashboard_id)
        
        # Recursively find links from each linked dashboard
        for linked_id in direct_links:
            if linked_id not in visited:
                # Get nested links first (depth-first)
                nested = self.find_all_linked_dashboards(linked_id, visited)
                result.extend(nested)
                # Then add this linked dashboard
                if linked_id not in result:
                    result.append(linked_id)
        
        return result
    
    def clone_with_all_linked(self, source_dashboard_id: int, new_name: str,
                              new_database_id: int, dashboard_collection_id: int = None,
                              questions_collection_id: int = None,
                              main_dashboard_collection_id: int = None) -> dict:
        """
        Clone a dashboard AND all its linked dashboards with the new database.
        
        This will:
        1. Find all dashboards linked from the source (recursively)
        2. Clone linked dashboards first (so we have their new IDs)
        3. Clone the main dashboard with click behaviors pointing to new dashboards
        
        Args:
            main_dashboard_collection_id: If provided, the MAIN dashboard goes here,
                                         linked dashboards go to dashboard_collection_id
        
        Returns the main cloned dashboard.
        """
        logger.info("="*70)
        logger.info("CLONING DASHBOARD WITH ALL LINKED DASHBOARDS")
        logger.info("="*70)
        
        # Reset mappings for fresh clone
        self.dashboard_mapping = {}
        self.question_mapping = {}
        self.tab_mapping = {}
        self.dashboard_tab_mappings = {}
        
        # Find all linked dashboards (in order: deepest first)
        logger.info(f"\nFinding all linked dashboards from {source_dashboard_id}...")
        all_linked = self.find_all_linked_dashboards(source_dashboard_id)
        
        if all_linked:
            logger.info(f"Found {len(all_linked)} linked dashboards: {all_linked}")
            logger.info("Will clone in order: linked dashboards first, then main dashboard")
        else:
            logger.info("No linked dashboards found - will clone single dashboard")
        
        # Clone order: linked dashboards first, main dashboard last
        dashboards_to_clone = all_linked + [source_dashboard_id]
        
        # Get names of dashboards to clone
        dashboard_names = {}
        for dash_id in dashboards_to_clone:
            dash = self.manager.get_dashboard(dash_id)
            if dash:
                dashboard_names[dash_id] = dash.get('name', f'Dashboard {dash_id}')
        
        logger.info(f"\nDashboards to clone ({len(dashboards_to_clone)}):")
        for i, dash_id in enumerate(dashboards_to_clone, 1):
            logger.info(f"  {i}. {dashboard_names.get(dash_id, dash_id)} (ID: {dash_id})")
        
        logger.info("\n" + "="*70)
        
        # Clone each dashboard
        main_dashboard = None
        for i, dash_id in enumerate(dashboards_to_clone, 1):
            original_name = dashboard_names.get(dash_id, f'Dashboard {dash_id}')
            
            # Create new name
            if dash_id == source_dashboard_id:
                clone_name = new_name
            else:
                # For linked dashboards, prefix with new name
                clone_name = f"{new_name} - {original_name}"
            
            logger.info(f"\n[{i}/{len(dashboards_to_clone)}] Cloning: {original_name}")
            logger.info(f"    New name: {clone_name}")
            
            try:
                # Main dashboard goes to _DASHBOARDS collection if provided
                # Linked dashboards go to the customer collection
                if dash_id == source_dashboard_id and main_dashboard_collection_id:
                    target_collection = main_dashboard_collection_id
                else:
                    target_collection = dashboard_collection_id
                
                new_dashboard = self.clone_dashboard(
                    source_dashboard_id=dash_id,
                    new_name=clone_name,
                    new_database_id=new_database_id,
                    dashboard_collection_id=target_collection,
                    questions_collection_id=questions_collection_id
                )
                
                if new_dashboard:
                    logger.info(f"    Created: ID {new_dashboard['id']}")
                    if dash_id == source_dashboard_id:
                        main_dashboard = new_dashboard
                        
            except Exception as e:
                logger.error(f"    Failed: {e}")
        
        # SECOND PASS: Update click behaviors with complete mapping
        # This is needed because when cloning inner dashboards, the main dashboard
        # and other dashboards cloned after them weren't in the mapping yet
        # Also updates tab references in click behaviors
        logger.info("\n" + "-"*70)
        logger.info("UPDATING CLICK BEHAVIORS WITH COMPLETE MAPPING...")
        logger.info("-"*70)
        
        # Log available tab mappings for debugging
        if self.dashboard_tab_mappings:
            logger.info(f"  Available tab mappings for {len(self.dashboard_tab_mappings)} dashboards:")
            for dash_id, tab_map in self.dashboard_tab_mappings.items():
                logger.info(f"    Dashboard {dash_id}: {tab_map}")
        
        for old_id, new_id in self.dashboard_mapping.items():
            try:
                # Pass tab_mapping as fallback, but _remap_single_click_behavior will
                # use dashboard_tab_mappings for the target dashboard when available
                self.update_dashboard_click_behaviors(new_id, self.tab_mapping)
                logger.info(f"  Updated click behaviors for dashboard {new_id}")
            except Exception as e:
                logger.error(f"  Failed to update click behaviors for {new_id}: {e}")
        
        # Summary
        logger.info("\n" + "="*70)
        logger.info("CLONE COMPLETE!")
        logger.info("="*70)
        logger.info(f"Dashboards cloned: {len(self.dashboard_mapping)}")
        logger.info(f"Questions cloned: {len(self.question_mapping)}")
        logger.info("\nDashboard ID mapping (old -> new):")
        for old_id, new_id in self.dashboard_mapping.items():
            name = dashboard_names.get(old_id, f'Dashboard {old_id}')
            logger.info(f"  {old_id} -> {new_id} ({name})")
        logger.info("="*70)
        
        return main_dashboard


def main():
    """Interactive main function"""
    print("\n" + "="*70)
    print("DASHBOARD CLONE WITH DATABASE CHANGE")
    print("="*70)
    print("\nThis will:")
    print("  1. Clone a dashboard with all its questions")
    print("  2. Remap all questions to use a new database")
    print("  3. Auto-create collection for the customer")
    print("  4. Preserve filters, layouts, tabs, click behaviors")
    print("\n")
    
    config = load_config()
    if not config:
        print("ERROR: Please edit metabase_config.json with your credentials")
        return
    
    cloner = DashboardCloner(config)
    
    print("Connecting to Metabase...")
    if not cloner.authenticate():
        print("ERROR: Authentication failed!")
        return
    print("Connected!\n")
    
    # Get source dashboard
    source_id = input("Source Dashboard ID: ").strip()
    if not source_id.isdigit():
        print("ERROR: Invalid ID")
        return
    source_id = int(source_id)
    
    # Analyze linked dashboards - automatically clone all linked dashboards
    print("\nAnalyzing dashboard for linked dashboards...")
    all_linked = cloner.find_all_linked_dashboards(source_id)
    clone_all_linked = True  # Always clone linked dashboards
    
    if all_linked:
        print(f"This dashboard links to {len(all_linked)} other dashboard(s): {all_linked}")
        print("Will clone all linked dashboards!")
    else:
        print("No linked dashboards found.")
    
    # Get target database
    print("\n" + "-"*40)
    while True:
        db_name = input("Target Database Name: ").strip()
        if not db_name:
            print("Database name is required")
            continue
        
        target_db, suggestions = cloner.find_database(db_name)
        if target_db:
            print(f"Found database: {target_db['name']} (ID: {target_db['id']})")
            break
        else:
            print(f"Database '{db_name}' not found!")
            if suggestions:
                print("Did you mean one of these?")
                for i, s in enumerate(suggestions, 1):
                    print(f"  {i}. {s}")
                choice = input("Enter number to select, or type a new name: ").strip()
                if choice.isdigit() and 1 <= int(choice) <= len(suggestions):
                    target_db, _ = cloner.find_database(suggestions[int(choice)-1])
                    if target_db:
                        print(f"Selected: {target_db['name']} (ID: {target_db['id']})")
                        break
    
    # Get customer name - used for both dashboard name and collection name
    print("\n" + "-"*40)
    customer_name = input("Customer Name: ").strip()
    if not customer_name:
        print("ERROR: Customer name is required")
        return
    
    # Generate dashboard name and collection name from customer name
    new_name = f"{customer_name} Dashboard"
    collection_name = f"{customer_name} Collection"
    
    print(f"Dashboard will be named: {new_name}")
    print(f"Collection will be named: {collection_name}")
    
    # Get source dashboard's parent collection
    source_parent_collection = cloner.get_dashboard_collection_id(source_id)
    
    # Create customer collection in the same parent as source dashboard
    col = cloner.get_or_create_collection(collection_name, source_parent_collection)
    if col:
        collection_id = col['id']
        print(f"Customer collection: {col['name']} (ID: {col['id']})")
    else:
        collection_id = None
        print("Warning: Could not create collection, will use root")
    
    # Find "_DASHBOARDS" collection in the same parent for main dashboard
    dashboards_collection_id = None
    collections = cloner.manager.get_collections()
    for c in collections:
        if c.get('name') == '_DASHBOARDS' and c.get('location', '').rstrip('/').endswith(f"/{source_parent_collection}") if source_parent_collection else c.get('location') in ['/', None, '']:
            dashboards_collection_id = c['id']
            print(f"Main dashboard collection: _DASHBOARDS (ID: {dashboards_collection_id})")
            break
    
    # If not found by location, try simpler approach - find by parent_id
    if not dashboards_collection_id:
        for c in collections:
            if c.get('name') == '_DASHBOARDS':
                # Check if parent matches
                parent_id = c.get('parent_id')
                if parent_id == source_parent_collection:
                    dashboards_collection_id = c['id']
                    print(f"Main dashboard collection: _DASHBOARDS (ID: {dashboards_collection_id})")
                    break
    
    if not dashboards_collection_id:
        print("Warning: _DASHBOARDS collection not found, main dashboard will go to customer collection")
        dashboards_collection_id = collection_id
    
    # Use customer collection for linked dashboards and questions
    dash_collection_id = collection_id
    q_collection_id = collection_id
    
    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print(f"  Source Dashboard: {source_id}")
    print(f"  Customer: {customer_name}")
    print(f"  Dashboard Name: {new_name}")
    print(f"  Target Database: {target_db['name']} (ID: {target_db['id']})")
    print(f"  Main Dashboard -> _DASHBOARDS collection")
    print(f"  Linked Dashboards & Questions -> {collection_name}")
    if clone_all_linked and all_linked:
        print(f"  Linked Dashboards to clone: {len(all_linked)}")
    print("="*70)
    
    confirm = input("\nProceed? (yes/no): ").strip().lower()
    if confirm not in ['yes', 'y']:
        print("Cancelled")
        return
    
    print("\n")
    try:
        if clone_all_linked and all_linked:
            # Clone with all linked dashboards
            # Main dashboard goes to _DASHBOARDS, linked dashboards go to customer collection
            new_dashboard = cloner.clone_with_all_linked(
                source_dashboard_id=source_id,
                new_name=new_name,
                new_database_id=target_db['id'],
                dashboard_collection_id=dash_collection_id,
                questions_collection_id=q_collection_id,
                main_dashboard_collection_id=dashboards_collection_id
            )
        else:
            # Clone single dashboard - goes to _DASHBOARDS collection
            new_dashboard = cloner.clone_dashboard(
                source_dashboard_id=source_id,
                new_name=new_name,
                new_database_id=target_db['id'],
                dashboard_collection_id=dashboards_collection_id,
                questions_collection_id=q_collection_id
            )
        
        print("\n" + "="*70)
        print("SUCCESS!")
        print("="*70)
        print(f"  New Dashboard: {new_name}")
        print(f"  Dashboard ID: {new_dashboard['id']}")
        print(f"  Database: {target_db['name']}")
        print(f"  URL: {config['base_url']}/dashboard/{new_dashboard['id']}")
        if clone_all_linked and all_linked:
            print(f"  Total dashboards cloned: {len(cloner.dashboard_mapping)}")
        print("="*70)
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()


def run_clone(source_id: int, customer_name: str, database_name: str, 
              clone_linked: bool = True):
    """Non-interactive clone function"""
    config = load_config()
    if not config:
        print("ERROR: Please edit metabase_config.json with your credentials")
        return None
    
    cloner = DashboardCloner(config)
    
    print("Connecting to Metabase...")
    if not cloner.authenticate():
        print("ERROR: Authentication failed!")
        return None
    print("Connected!\n")
    
    # Find database
    target_db, suggestions = cloner.find_database(database_name)
    if not target_db:
        print(f"ERROR: Database '{database_name}' not found!")
        if suggestions:
            print("Did you mean one of these?")
            for s in suggestions:
                print(f"  - {s}")
        return None
    
    print(f"Target database: {target_db['name']} (ID: {target_db['id']})")
    
    # Generate names from customer name
    new_name = f"{customer_name} Dashboard"
    collection_name = f"{customer_name} Collection"
    print(f"Dashboard name: {new_name}")
    print(f"Collection name: {collection_name}")
    
    # Analyze linked dashboards
    all_linked = cloner.find_all_linked_dashboards(source_id)
    if all_linked:
        print(f"Dashboard links to {len(all_linked)} dashboards: {all_linked}")
        if clone_linked:
            print("Will clone all linked dashboards!")
    
    # Get or create collection in same parent as source dashboard
    source_parent = cloner.get_dashboard_collection_id(source_id)
    col = cloner.get_or_create_collection(collection_name, source_parent)
    collection_id = col['id'] if col else None
    if col:
        print(f"Customer collection: {col['name']} (ID: {col['id']})")
    
    # Find "_DASHBOARDS" collection in the same parent for main dashboard
    dashboards_collection_id = None
    collections = cloner.manager.get_collections()
    for c in collections:
        if c.get('name') == '_DASHBOARDS' and c.get('parent_id') == source_parent:
            dashboards_collection_id = c['id']
            print(f"Main dashboard collection: _DASHBOARDS (ID: {dashboards_collection_id})")
            break
    
    if not dashboards_collection_id:
        print("Warning: _DASHBOARDS collection not found, main dashboard will go to customer collection")
        dashboards_collection_id = collection_id
    
    dash_collection_id = collection_id
    q_collection_id = collection_id
    
    # Clone
    print("\nStarting clone...")
    
    if clone_linked and all_linked:
        # Clone with all linked dashboards
        # Main dashboard goes to _DASHBOARDS, linked dashboards go to customer collection
        new_dashboard = cloner.clone_with_all_linked(
            source_dashboard_id=source_id,
            new_name=new_name,
            new_database_id=target_db['id'],
            dashboard_collection_id=dash_collection_id,
            questions_collection_id=q_collection_id,
            main_dashboard_collection_id=dashboards_collection_id
        )
    else:
        # Clone single dashboard - goes to _DASHBOARDS collection
        new_dashboard = cloner.clone_dashboard(
            source_dashboard_id=source_id,
            new_name=new_name,
            new_database_id=target_db['id'],
            dashboard_collection_id=dashboards_collection_id,
            questions_collection_id=q_collection_id
        )
    
    if new_dashboard:
        print("\n" + "="*70)
        print("SUCCESS!")
        print("="*70)
        print(f"  New Dashboard: {new_name}")
        print(f"  Dashboard ID: {new_dashboard['id']}")
        print(f"  Database: {target_db['name']}")
        print(f"  URL: {config['base_url']}/dashboard/{new_dashboard['id']}")
        if clone_linked and all_linked:
            print(f"  Total dashboards cloned: {len(cloner.dashboard_mapping)}")
        print("="*70)
    
    return new_dashboard


def diagnose_dashboard(dashboard_id: int):
    """Run click behavior diagnosis on a dashboard"""
    config = load_config()
    if not config:
        print("ERROR: Please edit metabase_config.json with your credentials")
        return
    
    cloner = DashboardCloner(config)
    
    print("Connecting to Metabase...")
    if not cloner.authenticate():
        print("ERROR: Authentication failed!")
        return
    print("Connected!\n")
    
    cloner.diagnose_click_behaviors(dashboard_id)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Clone Metabase dashboard with database change')
    parser.add_argument('--source', '-s', type=int, help='Source dashboard ID')
    parser.add_argument('--customer', '-c', type=str, help='Customer name (used for dashboard and collection names)')
    parser.add_argument('--database', '-d', type=str, help='Target database name')
    parser.add_argument('--diagnose', type=int, help='Diagnose click behaviors in a dashboard (provide dashboard ID)')
    
    args = parser.parse_args()
    
    # If diagnose mode
    if args.diagnose:
        diagnose_dashboard(args.diagnose)
    # If all required args provided, run non-interactively
    elif args.source and args.customer and args.database:
        try:
            run_clone(
                source_id=args.source,
                customer_name=args.customer,
                database_name=args.database
            )
        except Exception as e:
            print(f"ERROR: {e}")
            import traceback
            traceback.print_exc()
    else:
        # Interactive mode
        try:
            main()
        except KeyboardInterrupt:
            print("\n\nCancelled")
