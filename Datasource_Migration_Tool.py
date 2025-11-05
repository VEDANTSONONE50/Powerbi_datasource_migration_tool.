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
        margin-bottom: 2rem;
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
    </style>
""", unsafe_allow_html=True)

# Define the migration function BEFORE using it
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
    
    st.session_state.processing_complete = True

# Title
st.markdown('<div class="main-header">🔄 Power BI to Fabric Migration Tool</div>', unsafe_allow_html=True)

# Initialize session state
if 'files_discovered' not in st.session_state:
    st.session_state.files_discovered = []
if 'table_mapping' not in st.session_state:
    st.session_state.table_mapping = {}
if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = False

# Sidebar for configuration
with st.sidebar:
    st.header("⚙️ Configuration")
    
    st.subheader("📁 File Location")
    table_folder = st.text_input(
        "Table Folder Path",
        value="",
        help="Path to the folder containing .tmdl files ; e.g :- C:Vedant\\Desktop\\ReportName.Semantic.Model\\definations\\tables",
        placeholder=" C:User\\Path\\to\\ReportName.Semantic.Model\\definations\\tables"
    )
    
    st.subheader("🔐 Database Credentials")
    sql_server = st.text_input(
        "SQL Server",
        value="",
        help="Fabric lakehouse ID / SQL Server address",
        placeholder="your-server.datawarehouse.fabric.microsoft.com"
    )
    
    sql_database = st.text_input(
        "SQL Database",
        value="",
        help="Lakehouse name / Database name",
        placeholder="YourLakehouseName"
    )
    
    st.divider()
    
    # Validate inputs before allowing discovery
    can_discover = table_folder and sql_server and sql_database
    
    if not can_discover:
        st.warning("⚠️ Please fill in all configuration fields above")
    
    if st.button("🔍 Discover Files", use_container_width=True, disabled=not can_discover):
        if os.path.exists(table_folder):
            files = [f for f in os.listdir(table_folder) if f.endswith(".tmdl")]
            
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
                    file_path = os.path.join(table_folder, file_name)
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
                    st.session_state.processing_complete = False
                    st.success(f"✅ Found {len(files_to_process)} files to process")
                else:
                    st.error("❌ No files with partition patterns found")
        else:
            st.error("❌ Folder path does not exist")
    
    # Show current configuration
    if sql_server and sql_database:
        st.divider()
        st.subheader("📋 Current Config")
        st.text(f"Server: {sql_server[:30]}...")
        st.text(f"Database: {sql_database}")

# Main content area
if not st.session_state.files_discovered:
    st.markdown('<div class="info-box">', unsafe_allow_html=True)
    st.markdown("""
    ### 👋 Welcome to the Migration Tool
    
    This tool helps you migrate Power BI semantic models to Microsoft Fabric by:
    - Converting Excel/CSV sources to SQL Database connections
    - Mapping table names to your Fabric lakehouse tables
    - Automatically handling data transformations
    
    **To get started:**
    1. Configure your settings in the sidebar (folder path, server, database)
    2. Click "🔍 Discover Files" to find .tmdl files
    3. Map your table names
    4. Click "▶️ Run Migration" to process
    
    #### 📖 Example Configuration:
    - **Server**: `abc123.datawarehouse.fabric.microsoft.com`
    - **Database**: `MyLakehouse`
    - **Folder**: `C:\\Path\\To\\SemanticModel\\definition\\tables`
    """)
    st.markdown('</div>', unsafe_allow_html=True)
else:
    # Display discovered files
    st.header("📋 Table Name Mapping")
    st.markdown(f"**Found {len(st.session_state.files_discovered)} files** that will be processed:")
    
    # Create tabs for better organization
    tab1, tab2 = st.tabs(["🗂️ Table Mapping", "📊 Summary"])
    
    with tab1:
        st.markdown("Configure the target table names for each file:")
        
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
                    help=f"Enter the table name in your Fabric database"
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
        **Configuration:**
        - **Server:** {sql_server}
        - **Database:** {sql_database}
        - **Files to Process:** {len(st.session_state.files_discovered)}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Run migration button
    st.divider()
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        if st.button("▶️ Run Migration", use_container_width=True, type="primary"):
            run_migration(
                table_folder,
                sql_server,
                sql_database,
                st.session_state.files_discovered,
                st.session_state.table_mapping
            )

# Footer
st.divider()
st.markdown("""
    <div style="text-align: center; color: #666; padding: 1rem;">
        <small>Power BI to Microsoft Fabric Migration Tool | Built with Streamlit</small>
    </div>
""", unsafe_allow_html=True)
