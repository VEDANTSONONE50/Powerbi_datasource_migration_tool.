import streamlit as st
import os
import re
from pathlib import Path

# Page configuration
st.set_page_config(
    page_title="Power BI to Fabric Migration Tool",
    page_icon="🔄",
    layout="wide"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
    }
    .success-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
        margin: 1rem 0;
    }
    .warning-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        color: #856404;
        margin: 1rem 0;
    }
    .info-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d1ecf1;
        border: 1px solid #bee5eb;
        color: #0c5460;
        margin: 1rem 0;
    }
    .step-indicator {
        text-align: center;
        padding: 1rem;
        margin-bottom: 2rem;
    }
    .step {
        display: inline-block;
        padding: 0.5rem 1rem;
        margin: 0 0.5rem;
        border-radius: 20px;
        background-color: #e9ecef;
        color: #6c757d;
        font-weight: 500;
    }
    .step.active {
        background-color: #1f77b4;
        color: white;
    }
    .step.completed {
        background-color: #28a745;
        color: white;
    }
    </style>
""", unsafe_allow_html=True)

# Define the migration function
def run_migration(table_folder, sql_server, sql_database, files, table_mapping):
    """Execute the migration process"""
    
    # Patterns
    partition_pattern = re.compile(
        r'^\s*partition\s+(?:"[^"]+"|\'[^\']+\'|\S+)\s*=\s*m\b',
        re.MULTILINE
    )
    
    source_block_pattern = re.compile(
        r'(\n\s*source\s*=)(.*?)(?=\n\s*annotation|\Z)', re.DOTALL
    )
    
    two_line_pattern = re.compile(
        r'(?P<indent>\s*)Source\s*=\s*.+?,\s*\n(?P=indent)(?P<var_name>.+?)\s*=\s*Source\{[^\}]+\}\[Data\]',
        re.IGNORECASE
    )
    
    promoted_headers_pattern = re.compile(
        r'^\s*(?P<step>#"[^"]*"|\w+)\s*=\s*Table\.PromoteHeaders\(\s*(?P<input_step>#"[^"]*"|\w+)\s*,[^\)]*\),?\s*\n',
        re.MULTILINE
    )
    
    csv_pattern = re.compile(
        r'Source\s*=\s*Csv\.Document\(',
        re.IGNORECASE
    )
    
    def replacer_func(match, table_name):
        source_prefix = match.group(1)
        block = match.group(2)

        def two_line_replacer(m):
            indent = m.group("indent")
            var_name = m.group("var_name")
            return (
                f'{indent}Source = Sql.Database("{sql_server}", "{sql_database}"),\n'
                f'{indent}{var_name} = Source{{[Schema="dbo",Item="{table_name}"]}}[Data]'
            )
        
        if csv_pattern.search(block):
            csv_replacer_pattern = re.compile(
                r'Source\s*=\s*Csv\.Document\(',
                re.IGNORECASE
            )
            
            matches = list(csv_replacer_pattern.finditer(block))
            for match in reversed(matches):
                start = match.start()
                text_from_start = block[start:]
                
                paren_count = 0
                in_string = False
                escape_next = False
                end_pos = 0
                
                for i, char in enumerate(text_from_start):
                    if escape_next:
                        escape_next = False
                        continue
                    if char == '\\':
                        escape_next = True
                        continue
                    if char == '"':
                        in_string = not in_string
                        continue
                    if not in_string:
                        if char == '(':
                            paren_count += 1
                        elif char == ')':
                            paren_count -= 1
                            if paren_count == 0:
                                end_pos = i + 1
                                break
                
                block = block[:start] + f'Source = Sql.Database("{sql_server}", "{sql_database}")' + block[start + end_pos:]
            
            var_name = table_name.replace(" ", "_").replace("(", "").replace(")", "")
            
            promoted_headers_csv_pattern = re.compile(
                r'#"Promoted Headers"\s*=\s*Table\.PromoteHeaders\(Source,[^\)]*\)',
                re.IGNORECASE
            )
            
            def csv_var_replacer(m):
                return f'{var_name} = Source{{[Schema="dbo",Item="{table_name}"]}}[Data]'
            
            block = promoted_headers_csv_pattern.sub(csv_var_replacer, block)
            
            block = re.sub(
                r'#"Promoted Headers"',
                var_name,
                block,
                flags=re.IGNORECASE
            )
        else:
            block = two_line_pattern.sub(two_line_replacer, block)

        is_csv = csv_pattern.search(block)
        if not is_csv:
            promoted = promoted_headers_pattern.search(block)
            if promoted:
                promoted_step = promoted.group("step")
                input_step = promoted.group("input_step")
                block = promoted_headers_pattern.sub("", block)
                block = re.sub(
                    rf'(?<!\w){re.escape(promoted_step)}(?!\w)',
                    input_step,
                    block
                )

        return source_prefix + block
    
    # Progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    processed_count = 0
    results = []
    
    for idx, file_name in enumerate(files):
        file_path = os.path.join(table_folder, file_name)
        
        status_text.text(f"Processing {idx + 1}/{len(files)}: {file_name}")
        
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            if not partition_pattern.search(content):
                results.append({"file": file_name, "status": "⚠️ Skipped (no partition match)"})
                continue

            table_name = table_mapping.get(file_name, os.path.splitext(file_name)[0])
            updated = source_block_pattern.sub(lambda m: replacer_func(m, table_name), content)

            if updated == content:
                results.append({"file": file_name, "status": "⚠️ No changes needed"})
                continue

            # Backup original
            backup_path = file_path + ".backup"
            with open(backup_path, "w", encoding="utf-8") as f:
                f.write(content)

            # Write updated content
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(updated)

            results.append({"file": file_name, "status": "✅ Successfully processed"})
            processed_count += 1
        except Exception as e:
            results.append({"file": file_name, "status": f"❌ Error: {str(e)}"})
        
        # Update progress
        progress_bar.progress((idx + 1) / len(files))
    
    status_text.empty()
    progress_bar.empty()
    
    # Display results
    st.markdown('<div class="success-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### ✅ Migration Complete!
    
    **{processed_count} out of {len(files)} files** were successfully processed.
    
    Backup files have been created with the `.backup` extension.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Results table
    st.subheader("📝 Processing Results")
    st.table(results)
    
    return True

# Initialize session state
if 'current_step' not in st.session_state:
    st.session_state.current_step = 1
if 'files_discovered' not in st.session_state:
    st.session_state.files_discovered = []
if 'table_mapping' not in st.session_state:
    st.session_state.table_mapping = {}
if 'table_folder' not in st.session_state:
    st.session_state.table_folder = ""
if 'sql_server' not in st.session_state:
    st.session_state.sql_server = ""
if 'sql_database' not in st.session_state:
    st.session_state.sql_database = ""
if 'migration_complete' not in st.session_state:
    st.session_state.migration_complete = False

# Title
st.markdown('<div class="main-header">🔄 Power BI to Fabric Migration Tool</div>', unsafe_allow_html=True)

# Step indicator
steps = ["Configuration", "File Discovery", "Table Mapping", "Migration"]
step_html = '<div class="step-indicator">'
for idx, step in enumerate(steps, 1):
    if idx < st.session_state.current_step:
        step_class = "step completed"
    elif idx == st.session_state.current_step:
        step_class = "step active"
    else:
        step_class = "step"
    step_html += f'<span class="{step_class}">{idx}. {step}</span>'
step_html += '</div>'
st.markdown(step_html, unsafe_allow_html=True)

# STEP 1: Configuration
if st.session_state.current_step == 1:
    st.header("⚙️ Step 1: Configuration")
    
    st.markdown('<div class="info-box">', unsafe_allow_html=True)
    st.markdown("""
    ### 📖 Configuration Guide
    
    Please provide the following information to configure your migration:
    
    - **Table Folder Path**: Path to your .tmdl files (e.g., `C:\\Users\\Path\\ReportName.SemanticModel\\definition\\tables`)
    - **Fabric Server ID**: Your Fabric lakehouse server address (e.g., `abc123.datawarehouse.fabric.microsoft.com`)
    - **Fabric Database Name**: Name of your lakehouse (e.g., `MyLakehouse`)
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("📁 File Location")
        table_folder = st.text_input(
            "Table Folder Path",
            value=st.session_state.table_folder,
            help="Path to the folder containing .tmdl files",
            placeholder="C:\\User\\Path\\to\\ReportName.SemanticModel\\definition\\tables"
        )
        st.session_state.table_folder = table_folder
    
    with col2:
        st.subheader("🔐 Database Credentials")
        sql_server = st.text_input(
            "Fabric Server ID",
            value=st.session_state.sql_server,
            help="Fabric lakehouse ID / SQL Server address",
            placeholder="your-server.datawarehouse.fabric.microsoft.com"
        )
        st.session_state.sql_server = sql_server
        
        sql_database = st.text_input(
            "Fabric Database Name",
            value=st.session_state.sql_database,
            help="Lakehouse name",
            placeholder="YourLakehouseName"
        )
        st.session_state.sql_database = sql_database
    
    st.divider()
    
    # Navigation buttons
    col1, col2, col3 = st.columns([1, 2, 1])
    with col3:
        can_proceed = table_folder and sql_server and sql_database
        if not can_proceed:
            st.warning("⚠️ Please fill in all fields")
        
        if st.button("Next →", use_container_width=True, type="primary", disabled=not can_proceed):
            if os.path.exists(table_folder):
                st.session_state.current_step = 2
                st.rerun()
            else:
                st.error("❌ Folder path does not exist")

# STEP 2: File Discovery
elif st.session_state.current_step == 2:
    st.header("🔍 Step 2: File Discovery")
    
    if not st.session_state.files_discovered:
        st.markdown('<div class="info-box">', unsafe_allow_html=True)
        st.markdown("""
        ### Discovering Files
        
        Click the button below to scan for .tmdl files in your specified folder.
        
        The tool will:
        - Search for all .tmdl files
        - Filter files with partition patterns
        - Prepare them for migration
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns([1, 1, 1])
        with col2:
            if st.button("🔍 Discover Files", use_container_width=True, type="primary"):
                with st.spinner("Scanning folder..."):
                    files = [f for f in os.listdir(st.session_state.table_folder) if f.endswith(".tmdl")]
                    
                    if not files:
                        st.error("❌ No .tmdl files found in the specified folder")
                    else:
                        # Patterns
                        partition_pattern = re.compile(
                            r'^\s*partition\s+(?:"[^"]+"|\'[^\']+\'|\S+)\s*=\s*m\b',
                            re.MULTILINE
                        )
                        
                        # Filter files with partition patterns
                        files_to_process = []
                        for file_name in files:
                            file_path = os.path.join(st.session_state.table_folder, file_name)
                            try:
                                with open(file_path, "r", encoding="utf-8") as f:
                                    content = f.read()
                                
                                if partition_pattern.search(content):
                                    files_to_process.append(file_name)
                            except Exception as e:
                                st.warning(f"Could not read {file_name}: {str(e)}")
                        
                        if files_to_process:
                            st.session_state.files_discovered = files_to_process
                            st.session_state.table_mapping = {
                                f: os.path.splitext(f)[0] for f in files_to_process
                            }
                            st.success(f"✅ Found {len(files_to_process)} files to process")
                            st.rerun()
                        else:
                            st.error("❌ No files with partition patterns found")
    else:
        st.markdown('<div class="success-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Files Discovered Successfully!
        
        Found **{len(st.session_state.files_discovered)} files** ready for migration.
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Display file list
        st.subheader("📄 Discovered Files")
        for idx, file_name in enumerate(st.session_state.files_discovered, 1):
            st.text(f"{idx}. {file_name}")
    
    st.divider()
    
    # Navigation buttons
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        if st.button("← Back", use_container_width=True):
            st.session_state.current_step = 1
            st.rerun()
    
    with col3:
        if st.button("Next →", use_container_width=True, type="primary", disabled=not st.session_state.files_discovered):
            st.session_state.current_step = 3
            st.rerun()

# STEP 3: Table Mapping
elif st.session_state.current_step == 3:
    st.header("🗂️ Step 3: Table Mapping")
    
    st.markdown('<div class="info-box">', unsafe_allow_html=True)
    st.markdown("""
    ### Configure Table Names
    
    Map each file to its corresponding table name in your Fabric database.
    The default mapping uses the file name without the extension.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Create tabs for better organization
    tab1, tab2 = st.tabs(["🗂️ Table Mapping", "📊 Summary"])
    
    with tab1:
        st.markdown("**Configure the target table names for each file:**")
        st.write("")
        
        # Create two columns for better layout
        col1, col2 = st.columns([1, 1])
        
        for idx, file_name in enumerate(st.session_state.files_discovered):
            default_table_name = os.path.splitext(file_name)[0]
            
            with col1 if idx % 2 == 0 else col2:
                st.markdown(f"**{idx + 1}. {file_name}**")
                new_table_name = st.text_input(
                    f"Table name",
                    value=st.session_state.table_mapping.get(file_name, default_table_name),
                    key=f"table_{idx}",
                    help=f"Enter the table name in your Fabric database",
                    label_visibility="collapsed"
                )
                st.session_state.table_mapping[file_name] = new_table_name
                st.markdown("---")
    
    with tab2:
        st.subheader("📊 Migration Summary")
        
        summary_data = []
        for file_name, table_name in st.session_state.table_mapping.items():
            summary_data.append({
                "File": file_name,
                "Target Table": table_name
            })
        
        st.table(summary_data)
        
        st.markdown('<div class="info-box">', unsafe_allow_html=True)
        st.markdown(f"""
        **Configuration Summary:**
        - **Fabric Server ID:** {st.session_state.sql_server}
        - **Fabric Database Name:** {st.session_state.sql_database}
        - **Files to Process:** {len(st.session_state.files_discovered)}
        - **Folder:** {st.session_state.table_folder}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    st.divider()
    
    # Navigation buttons
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        if st.button("← Back", use_container_width=True):
            st.session_state.current_step = 2
            st.rerun()
    
    with col3:
        if st.button("Next →", use_container_width=True, type="primary"):
            st.session_state.current_step = 4
            st.rerun()

# STEP 4: Migration
elif st.session_state.current_step == 4:
    st.header("▶️ Step 4: Run Migration")
    
    if not st.session_state.migration_complete:
        st.markdown('<div class="warning-box">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚠️ Ready to Migrate
        
        **Before proceeding:**
        - Ensure you have a backup of your files (automatic backups will be created)
        - Verify your table mappings are correct
        - Make sure the Fabric database is accessible
        
        Click the button below to start the migration process.
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Display summary
        st.subheader("📋 Final Review")
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Files to Process", len(st.session_state.files_discovered))
            st.metric("Fabric Server", st.session_state.sql_server)
        
        with col2:
            st.metric("Database", st.session_state.sql_database)
            st.metric("Folder", "✓ Configured")
        
        st.divider()
        
        # Navigation buttons
        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            if st.button("← Back", use_container_width=True):
                st.session_state.current_step = 3
                st.rerun()
        
        with col2:
            if st.button("▶️ Run Migration", use_container_width=True, type="primary"):
                success = run_migration(
                    st.session_state.table_folder,
                    st.session_state.sql_server,
                    st.session_state.sql_database,
                    st.session_state.files_discovered,
                    st.session_state.table_mapping
                )
                if success:
                    st.session_state.migration_complete = True
                    st.rerun()
    else:
        # Migration completed

        
        st.markdown('<div class="success-box">', unsafe_allow_html=True)
        st.markdown("""
        ### 🎉 Migration Complete!
        
        All files have been processed successfully. Backup files have been created with the `.backup` extension.
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.divider()
        
        # Navigation buttons
        col1, col2, col3 = st.columns([1, 1, 1])
        with col2:
            if st.button("🔄 Start New Migration", use_container_width=True, type="primary"):
                # Reset session state
                st.session_state.current_step = 1
                st.session_state.files_discovered = []
                st.session_state.table_mapping = {}
                st.session_state.migration_complete = False
                st.rerun()

# Footer
st.divider()
st.markdown("""
    <div style="text-align: center; color: #666; padding: 1rem;">
        <small>Power BI to Microsoft Fabric Migration Tool | Built with Streamlit</small>
    </div>
""", unsafe_allow_html=True)
