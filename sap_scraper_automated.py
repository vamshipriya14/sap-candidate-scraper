"""
SAP Scraper - Automated Version (Based on Working CDP Interceptor)
Fully automated with no manual intervention
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time
import json
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cdp_scraper.log'),
        logging.StreamHandler()
    ]
)

class SAPCDPScraper:
    def __init__(self, url):
        """Initialize scraper with CDP"""
        self.url = url
        self.all_candidates = []
        self.seen_candidates = set()
        self.captured_responses = []

        # Setup Chrome with password manager disabled
        options = webdriver.ChromeOptions()
        options.add_argument('--start-maximized')
        options.add_argument('--disable-save-password-bubble')

        # Additional arguments to prevent password manager dialog
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-password-generation')
        options.add_argument('--disable-password-manager-reauthentication')
        options.add_argument('--no-first-run')
        options.add_argument('--no-service-autorun')
        options.add_argument('--password-store=basic')

        # Disable password manager
        prefs = {
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
            "profile.default_content_setting_values.notifications": 2,
            "profile.default_content_setting_values.automatic_downloads": 1,
        }
        options.add_experimental_option("prefs", prefs)
        options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        options.add_experimental_option('useAutomationExtension', False)

        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, 10)

        # Remove webdriver property
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        # Enable CDP Network domain
        self.driver.execute_cdp_cmd('Network.enable', {})

    def login(self, company_id=None, agency_id=None, user_email=None, password=None):
        """Automated login"""
        import os
        import getpass

        # Get credentials from params, env vars, or input
        company_id = company_id or os.getenv('SAP_COMPANY_ID') or input("Company ID: ").strip()
        agency_id = agency_id or os.getenv('SAP_AGENCY_ID') or input("Agency ID: ").strip()
        user_email = user_email or os.getenv('SAP_USER_EMAIL') or input("User Email: ").strip()
        password = password or os.getenv('SAP_PASSWORD') or getpass.getpass("Password: ")

        self.driver.get(self.url)
        time.sleep(2)

        # Company ID
        logging.info("Logging in...")
        company_field = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='companyId']")))
        company_field.send_keys(company_id)
        continue_btn = self.driver.find_element(By.CSS_SELECTOR, "button[id*='continueButton']")
        continue_btn.click()
        time.sleep(3)

        # Credentials
        agency_field = self.wait.until(EC.presence_of_element_located((By.XPATH, "//input[contains(@placeholder, 'Agency ID')]")))
        agency_field.send_keys(agency_id)
        email_field = self.driver.find_element(By.XPATH, "//input[contains(@placeholder, 'Email')]")
        email_field.send_keys(user_email)
        pass_field = self.driver.find_element(By.CSS_SELECTOR, "input[type='password']")
        pass_field.send_keys(password)
        login_btn = self.driver.find_element(By.CSS_SELECTOR, "button[id*='login']")
        login_btn.click()
        time.sleep(5)

        logging.info("✓ Login completed")
        time.sleep(2)  # Wait for any dialogs to potentially appear

        # Try to dismiss any password dialogs (may or may not appear)
        try:
            # Press Escape key to dismiss dialogs
            from selenium.webdriver.common.keys import Keys
            self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
            time.sleep(1)
        except:
            pass

        # Click Candidates tab
        logging.info("Navigating to Candidates page...")
        try:
            candidates_tab = self.driver.find_element(By.XPATH, "//span[text()='Candidates']")
            candidates_tab.click()
        except:
            self.driver.execute_script("arguments[0].click();",
                self.driver.find_element(By.XPATH, "//span[text()='Candidates']"))
        time.sleep(3)

        logging.info("✓ Ready to extract candidates!")
        time.sleep(2)

    def get_network_responses(self):
        """Get all network responses from CDP"""
        try:
            # Execute JavaScript to access performance logs
            script = """
                var performance = window.performance || window.mozPerformance || window.msPerformance || window.webkitPerformance || {};
                var entries = performance.getEntries() || [];
                return entries.filter(e => e.name.includes('candidateList')).map(e => e.name);
            """
            urls = self.driver.execute_script(script)
            return urls
        except Exception as e:
            logging.warning(f"Error getting network responses: {e}")
            return []

    def scroll_and_capture(self):
        """Scroll left panel and let page load candidates"""
        logging.info("Scrolling and capturing data...")

        # Find the scrollable container
        try:
            container = self.driver.find_element(By.ID, "__xmlview2--candidateMaster-cont")
        except:
            try:
                container = self.driver.find_element(By.CSS_SELECTOR, "section.sapMPageEnableScrolling")
            except:
                logging.error("Could not find scroll container")
                return

        last_candidate_count = 0
        no_change_count = 0
        scroll_iteration = 0

        while no_change_count < 5:
            # Scroll down
            self.driver.execute_script(
                "arguments[0].scrollTop = arguments[0].scrollHeight",
                container
            )
            scroll_iteration += 1
            time.sleep(1.5)  # Wait for data to load

            # Count visible candidates
            candidates = self.driver.find_elements(By.CSS_SELECTOR, "li.sapMCLI")
            current_count = len(candidates)

            if current_count > last_candidate_count:
                logging.info(f"Loaded {current_count} candidates in left panel (scroll {scroll_iteration})...")
                last_candidate_count = current_count
                no_change_count = 0
            else:
                no_change_count += 1
                logging.info(f"No new candidates loaded (attempt {no_change_count}/5)")

        logging.info(f"✓ Finished loading. Total candidates in panel: {last_candidate_count}")
        return last_candidate_count

    def extract_via_javascript(self):
        """Use JavaScript to extract data directly from page's internal data"""
        logging.info("Extracting data using JavaScript...")

        script = """
        // Try to access SAP UI5 model data
        try {
            var view = sap.ui.getCore().byId("__xmlview2");
            if (!view) return null;

            var model = view.getModel();
            if (!model) return null;

            var data = model.getData();
            if (data && data.candidates) {
                return data.candidates;
            }

            // Try alternative path
            if (data && data.results) {
                return data.results;
            }

            return null;
        } catch (e) {
            return null;
        }
        """

        try:
            data = self.driver.execute_script(script)
            if data:
                logging.info(f"✓ Extracted {len(data)} candidates from page model!")
                return data
            else:
                logging.warning("Could not extract from page model")
                return None
        except Exception as e:
            logging.error(f"JavaScript extraction failed: {e}")
            return None

    def extract_by_clicking(self, max_candidates=None, existing_candidates=None):
        """
        Extract by clicking through candidates

        Args:
            max_candidates: Maximum number to process
            existing_candidates: Set of (email, phone, requisition_id) tuples already in DB
        """
        logging.info("Extracting by clicking through candidates...")

        candidates = self.driver.find_elements(By.CSS_SELECTOR, "li.sapMCLI")
        if max_candidates:
            candidates = candidates[:max_candidates]

        total = len(candidates)
        logging.info(f"Processing {total} candidates...")

        skipped_count = 0
        new_count = 0

        for idx, candidate in enumerate(candidates, 1):
            try:
                if idx % 100 == 0 or idx == 1:
                    logging.info(f"Processing candidate {idx}/{total}")

                # Click candidate
                try:
                    candidate.click()
                except:
                    self.driver.execute_script("arguments[0].click();", candidate)

                time.sleep(0.5)

                # Extract details
                candidate_data = self.extract_candidate_details()

                # Check if candidate is new (if existing_candidates set provided)
                if existing_candidates is not None:
                    for record in candidate_data:
                        # Create unique key
                        key = (
                            record.get('Email', '').lower().strip() if record.get('Email') else None,
                            record.get('Phone', '').strip() if record.get('Phone') else None,
                            record.get('Requisition_ID', '').strip() if record.get('Requisition_ID') else None
                        )

                        # Skip if already exists
                        if key in existing_candidates:
                            skipped_count += 1
                            logging.debug(f"Skipping existing candidate: {record.get('Name')}")
                        else:
                            self.all_candidates.append(record)
                            new_count += 1
                else:
                    # No filtering, add all
                    self.all_candidates.extend(candidate_data)
                    new_count += len(candidate_data)

                # Save progress
                if idx % 100 == 0:
                    self.save_progress(f"cdp_progress_{idx}.csv")

            except Exception as e:
                logging.error(f"Error at candidate {idx}: {e}")
                continue

        if existing_candidates is not None:
            logging.info(f"✓ Extracted {new_count} new records, skipped {skipped_count} existing")
        else:
            logging.info(f"✓ Extracted {len(self.all_candidates)} records")

    def extract_candidate_details(self):
        """Extract details from right pane"""
        try:
            candidate_info = {}

            # Name
            try:
                name = self.driver.find_element(By.XPATH,
                    "//span[contains(@class, 'sapUxAPObjectPageHeaderTitleText')] | //h2//span").text
                candidate_info['Name'] = name
            except:
                candidate_info['Name'] = ""

            # Email
            try:
                email = self.driver.find_element(By.XPATH,
                    "//div[@id[contains(., 'emailAddress')]]//span[contains(@id, '__text')]").text
                candidate_info['Email'] = email
            except Exception as e:
                logging.debug(f"Could not extract email: {e}")
                candidate_info['Email'] = ""

            # Phone
            try:
                phone = self.driver.find_element(By.XPATH,
                    "//div[@id[contains(., 'phoneNumber')]]//span[contains(@id, '__text')]").text
                candidate_info['Phone'] = phone
            except:
                candidate_info['Phone'] = ""

            # Created On
            try:
                created = self.driver.find_element(By.XPATH,
                    "//div[@id[contains(., 'createdOn')]]//span[contains(@id, '__text')]").text
                candidate_info['Created_On'] = created
            except:
                candidate_info['Created_On'] = ""

            # Rights Expire
            try:
                expire = self.driver.find_element(By.XPATH,
                    "//div[@id[contains(., 'rightsExpire')]]//span[contains(@id, '__text')]").text
                candidate_info['Rights_Expire'] = expire
            except:
                candidate_info['Rights_Expire'] = ""

            # Company
            try:
                company = self.driver.find_element(By.XPATH,
                    "//div[@id[contains(., 'currentCompany')]]//span[contains(@id, '__text')] | //div[@id[contains(., 'company')]]//span[contains(@id, '__text')]").text
                candidate_info['Company'] = company
            except:
                candidate_info['Company'] = ""

            # Job applications
            job_applications = []
            try:
                rows = self.driver.find_elements(By.XPATH,
                    "//tbody[contains(@id, 'candJobReqTable')]//tr[@role='row']")

                for row in rows:
                    try:
                        cells = row.find_elements(By.CSS_SELECTOR, "td[role='gridcell']")
                        if len(cells) >= 4:
                            job = candidate_info.copy()
                            job['Requisition_ID'] = cells[0].text.strip()
                            job['Job_Title'] = cells[1].text.strip()
                            job['Status'] = cells[2].text.strip()
                            job['Forwarded_On'] = cells[3].text.strip()
                            job_applications.append(job)
                    except:
                        continue
            except:
                pass

            if not job_applications:
                candidate_info['Requisition_ID'] = ""
                candidate_info['Job_Title'] = ""
                candidate_info['Status'] = ""
                candidate_info['Forwarded_On'] = ""
                job_applications.append(candidate_info)

            return job_applications

        except Exception as e:
            logging.error(f"Error extracting details: {e}")
            return []

    def save_progress(self, filename):
        """Save to CSV"""
        if self.all_candidates:
            df = pd.DataFrame(self.all_candidates)
            df.to_csv(filename, index=False)

    def save_to_excel(self, filename):
        """Save to Excel"""
        if self.all_candidates:
            df = pd.DataFrame(self.all_candidates)
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Candidates', index=False)

    def close(self):
        """Close browser"""
        self.driver.quit()


def clean_duplicate_name(name):
    """Remove duplicate names like 'John John' -> 'John'"""
    import re
    if not name or not isinstance(name, str):
        return name
    parts = re.split(r'\s{2,}', name.strip())
    if len(parts) == 2 and parts[0] == parts[1]:
        return parts[0]
    return name

def normalize_date(date_str):
    """
    Normalize SAP date format to YYYY-MM-DD for PostgreSQL

    Handles:
    - "15-Mar-2026" → "2026-03-15"
    - "15-03-2026" → "2026-03-15"
    - "2026-03-15" → "2026-03-15"
    """
    if not date_str or not isinstance(date_str, str):
        return None

    try:
        # Try parsing common formats
        formats = [
            '%d-%b-%Y',  # 15-Mar-2026 (SAP format)
            '%d-%b-%y',  # 15-Mar-26
            '%d-%m-%Y',  # 15-03-2026
            '%Y-%m-%d',  # 2026-03-15 (already normalized)
            '%d/%m/%Y',  # 15/03/2026
            '%m/%d/%Y',  # 03/15/2026
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue

        # If all parsing fails, return None (will be NULL in DB)
        logging.warning(f"Could not parse date: {date_str}")
        return None
    except Exception as e:
        logging.warning(f"Error normalizing date '{date_str}': {e}")
        return None

def get_existing_candidates(db_host, db_port, db_name, db_user, db_password, db_table):
    """
    Query database for existing candidates to avoid re-processing

    Returns:
        Set of (email, phone, requisition_id) tuples
    """
    import psycopg2

    try:
        conn = psycopg2.connect(
            host=db_host,
            port=int(db_port),
            database=db_name,
            user=db_user,
            password=db_password
        )
        cursor = conn.cursor()

        # Query all existing unique keys
        query = f"""
            SELECT LOWER(TRIM(email)), TRIM(phone), TRIM(requisition_id)
            FROM {db_table}
            WHERE email IS NOT NULL OR phone IS NOT NULL
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        # Create set of tuples for fast lookup
        existing = set(rows)

        cursor.close()
        conn.close()

        logging.info(f"📊 Found {len(existing)} existing candidates in database")
        return existing

    except Exception as e:
        logging.warning(f"⚠️  Could not query existing candidates: {e}")
        logging.warning("Will proceed without incremental filtering")
        return None


def main():
    import os

    # ===================================================================
    # CONFIGURATION
    # ===================================================================
    DB_INSERT = True  # Set to True to enable database insertion
    INCREMENTAL_MODE = True  # Set to True to skip existing candidates
    # ===================================================================

    print("=" * 70)
    print("🚀 SAP AUTOMATED SCRAPER - BASED ON WORKING CODE")
    print("=" * 70)
    print("\nUses the exact extraction logic that works!\n")

    URL = os.getenv('SAP_URL', 'https://agencysvc44.sapsf.com/login')

    # Get max candidates from env var or default to all
    max_candidates_env = os.getenv('SAP_MAX_CANDIDATES')
    if max_candidates_env:
        max_candidates = int(max_candidates_env)
    else:
        # Default to scraping all candidates (for GitHub Actions/non-interactive)
        max_candidates = None
        logging.info("SAP_MAX_CANDIDATES not set - will scrape all candidates")

    scraper = SAPCDPScraper(URL)

    try:
        # Login with automation
        scraper.login()

        # Get existing candidates if incremental mode enabled
        existing_candidates = None
        if INCREMENTAL_MODE and DB_INSERT:
            db_host = os.getenv('DB_HOST')
            db_port = os.getenv('DB_PORT', '5432')
            db_name = os.getenv('DB_NAME')
            db_user = os.getenv('DB_USER')
            db_password = os.getenv('DB_PASSWORD')
            db_table = os.getenv('DB_TABLE', 'candidates')

            if all([db_host, db_name, db_user, db_password]):
                existing_candidates = get_existing_candidates(
                    db_host, db_port, db_name, db_user, db_password, db_table
                )

        # Extract by clicking through candidates
        logging.info("Using click-through method...")
        scraper.extract_by_clicking(
            max_candidates=max_candidates,
            existing_candidates=existing_candidates
        )

        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"sap_cdp_candidates_{timestamp}.csv"
        excel_filename = f"sap_cdp_candidates_{timestamp}.xlsx"

        scraper.save_to_excel(excel_filename)
        scraper.save_progress(csv_filename)

        print("\n" + "=" * 70)
        print("✓ Extraction completed!")
        print(f"✓ Total records: {len(scraper.all_candidates)}")
        print(f"✓ Saved CSV: {csv_filename}")
        print(f"✓ Saved Excel: {excel_filename}")
        print("=" * 70)

        # Database insertion (controlled by DB_INSERT flag)
        if DB_INSERT:
            import os
            import psycopg2
            from psycopg2 import pool

            # Get database credentials
            db_host = os.getenv('DB_HOST')
            db_port = os.getenv('DB_PORT', '5432')
            db_name = os.getenv('DB_NAME')
            db_user = os.getenv('DB_USER')
            db_password = os.getenv('DB_PASSWORD')
            db_table = os.getenv('DB_TABLE', 'candidates')
            audit_username = os.getenv('AUDIT_USERNAME', 'sap_scraper')

            if all([db_host, db_name, db_user, db_password]):
                logging.info("\n" + "=" * 70)
                logging.info("📤 Inserting data to PostgreSQL...")
                logging.info("=" * 70)

                try:
                    # Create connection
                    conn = psycopg2.connect(
                        host=db_host,
                        port=int(db_port),
                        database=db_name,
                        user=db_user,
                        password=db_password
                    )
                    cursor = conn.cursor()

                    # Clean data before insertion
                    df = pd.DataFrame(scraper.all_candidates)

                    # Remove duplicate names (e.g., "John John" -> "John")
                    if 'Name' in df.columns:
                        df['Name'] = df['Name'].apply(lambda x: clean_duplicate_name(x) if pd.notna(x) else x)

                    # Trim whitespace for all text columns
                    for col in df.columns:
                        if df[col].dtype == 'object':
                            df[col] = df[col].str.strip() if hasattr(df[col], 'str') else df[col]

                    # Lowercase emails
                    if 'Email' in df.columns:
                        df['Email'] = df['Email'].str.lower()

                    # Convert to records for insertion
                    records = df.to_dict('records')

                    # Insert each record with audit fields
                    inserted_count = 0
                    duplicate_count = 0

                    for idx, record in enumerate(records, 1):
                        # Prepare fields and values (following hr-email-automation pattern)
                        fields = []
                        values = []
                        placeholders = []

                        # Map SAP fields to DB columns (adjust these based on your table schema)
                        field_mapping = {
                            'Name': 'name',
                            'Email': 'email',
                            'Phone': 'phone',
                            'Created_On': 'created_on',
                            'Rights_Expire': 'rights_expire',
                            'Requisition_ID': 'requisition_id',
                            'Job_Title': 'job_title',
                            'Status': 'status',
                            'Forwarded_On': 'forwarded_on'
                        }

                        # Add record fields
                        date_fields = {'Created_On', 'Rights_Expire', 'Forwarded_On'}

                        for sap_field, db_field in field_mapping.items():
                            if sap_field in record and record[sap_field]:
                                value = record[sap_field]

                                # Normalize dates to YYYY-MM-DD
                                if sap_field in date_fields:
                                    value = normalize_date(value)
                                    if not value:  # Skip if date normalization failed
                                        continue
                                # Trim text values
                                elif isinstance(value, str):
                                    value = value.strip()

                                if value:  # Only add if not empty
                                    fields.append(db_field)
                                    values.append(value)
                                    placeholders.append('%s')

                        # Add audit fields (following hr-email-automation pattern)
                        audit_time = datetime.now()

                        fields.extend(['created_by', 'created_date', 'modified_by', 'modified_date'])
                        values.extend([audit_username, audit_time, audit_username, audit_time])
                        placeholders.extend(['%s', '%s', '%s', '%s'])

                        # Build and execute INSERT query
                        query = f"""
                            INSERT INTO {db_table} ({', '.join(fields)})
                            VALUES ({', '.join(placeholders)})
                        """

                        try:
                            cursor.execute(query, tuple(values))
                            conn.commit()
                            inserted_count += 1

                            if idx % 100 == 0:
                                logging.info(f"✓ Inserted {idx}/{len(records)} records...")
                        except psycopg2.errors.UniqueViolation:
                            conn.rollback()
                            duplicate_count += 1
                            logging.debug(f"Duplicate found for record {idx}, skipping")
                        except Exception as e:
                            conn.rollback()
                            logging.error(f"❌ Error inserting record {idx}: {e}")

                    cursor.close()
                    conn.close()

                    print("\n" + "=" * 70)
                    print("✓ Database insertion completed:")
                    print(f"  - Inserted: {inserted_count} records")
                    print(f"  - Duplicates skipped: {duplicate_count} records")
                    print("=" * 70)

                except Exception as e:
                    logging.error(f"❌ Database connection error: {e}")
            else:
                logging.warning("⚠️  Database credentials not found - skipping database insertion")
        else:
            logging.info("ℹ️  Database insertion disabled (DB_INSERT = False)")

    except Exception as e:
        logging.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        scraper.close()


if __name__ == "__main__":
    main()
