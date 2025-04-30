import streamlit as st
import plotly.express as px
import pandas as pd
import logging
import json
import time
import traceback

logger = logging.getLogger(__name__)

class Visualizer:
    def __init__(self, config):
        """Initialize visualizer with configuration."""
        self.config = config
        self.colors = {
            'DEBUG': '#12133f',
            'INFO': '#38A169',
            'WARN': '#D69E2E',
            'ERROR': '#E53E3E',
            'FATAL': '#805AD5'
        }
        # Initialize session state
        if 'logs_to_display_class' not in st.session_state:
            st.session_state.logs_to_display_class = []
        if 'logs_to_display_service' not in st.session_state:
            st.session_state.logs_to_display_service = []
        if 'log_page' not in st.session_state:
            st.session_state.log_page = 1
            st.session_state.logs_per_page = 100
        if 'total_logs_class' not in st.session_state:
            st.session_state.total_logs_class = 0
        if 'total_logs_service' not in st.session_state:
            st.session_state.total_logs_service = 0
        if 'selected_class' not in st.session_state:
            st.session_state.selected_class = None
        if 'selected_service' not in st.session_state:
            st.session_state.selected_service = None
        if 'selected_class_level' not in st.session_state:
            st.session_state.selected_class_level = None
        if 'selected_service_level' not in st.session_state:
            st.session_state.selected_service_level = None
        if 'reset_regex_class' not in st.session_state:
            st.session_state.reset_regex_class = False
        if 'reset_regex_service' not in st.session_state:
            st.session_state.reset_regex_service = False

    def _update_class_selection(self):
        """Callback to update selected class."""
        st.session_state.selected_class = st.session_state.class_select

    def _update_service_selection(self):
        """Callback to update selected service."""
        st.session_state.selected_service = st.session_state.service_select

    def _update_class_level_selection(self):
        """Callback to update selected class level."""
        st.session_state.selected_class_level = st.session_state.class_level_select

    def _update_service_level_selection(self):
        """Callback to update selected service level."""
        st.session_state.selected_service_level = st.session_state.service_level_select

    @st.cache_data
    def _create_timeline_figure(_self, timeline_data):
        """Create cached timeline figure."""
        fig = px.line(
            timeline_data,
            x='timestamp',
            y='count',
            color='level',
            title="Log Levels Over Time",
            color_discrete_map=_self.colors
        )
        fig.update_layout(
            xaxis_title="Time",
            yaxis_title="Count",
            plot_bgcolor='#f0ede6',
            paper_bgcolor='#f0ede6',
            font_color='#1A1A1A',
            title_font_color='#12133f',
            legend_title_font_color='#1A1A1A',
            font_size=14,
            height=400,
            margin=dict(t=50, b=50, l=50, r=50)
        )
        return fig

    @st.cache_data
    def _create_pie_figure(_self, data, names, values, title):
        """Create cached pie figure."""
        fig = px.pie(
            data,
            values=values,
            names=names,
            title=title,
            color_discrete_sequence=px.colors.qualitative.Plotly
        )
        fig.update_traces(textinfo='percent+label', textfont_size=14)
        fig.update_layout(
            plot_bgcolor='#f0ede6',
            paper_bgcolor='#f0ede6',
            font_color='#1A1A1A',
            title_font_color='#12133f',
            legend_title_font_color='#1A1A1A',
            font_size=14,
            height=350,
            margin=dict(t=50, b=50, l=50, r=50)
        )
        return fig

    def _render_log_form(self, df, table_type, log_processor):
        """Render form with dropdowns, button, and paginated log table."""
        index_col = 'class' if table_type == 'class' else 'service'
        logs_to_display = (st.session_state.logs_to_display_class if table_type == 'class'
                           else st.session_state.logs_to_display_service)
        total_logs = (st.session_state.total_logs_class if table_type == 'class'
                      else st.session_state.total_logs_service)
        selected_index = (st.session_state.selected_class if table_type == 'class'
                          else st.session_state.selected_service)
        selected_level = (st.session_state.selected_class_level if table_type == 'class'
                          else st.session_state.selected_service_level)

        if (st.session_state.get('app_state') != 'RUNNING' and 
            not df.empty and 
            df.iloc[:, 1:].sum().sum() > 0):
            with st.container():
                st.markdown(f"### Select {table_type.capitalize()} and Log Level")
                col1, col2 = st.columns(2)
                with col1:
                    default_index = df[index_col].tolist().index(selected_index) if selected_index in df[index_col].tolist() else 0
                    st.selectbox(
                        f"Select {table_type.capitalize()}",
                        options=df[index_col],
                        index=default_index,
                        key=f"{table_type}_select",
                        on_change=self._update_class_selection if table_type == 'class' else self._update_service_selection
                    )
                with col2:
                    default_level = self.config['app']['log_levels'].index(selected_level) if selected_level in self.config['app']['log_levels'] else 0
                    st.selectbox(
                        "Select Log Level",
                        options=self.config['app']['log_levels'],
                        index=default_level,
                        key=f"{table_type}_level_select",
                        on_change=self._update_class_level_selection if table_type == 'class' else self._update_service_level_selection
                    )
                st.markdown(
                    """
                    <style>
                    .fetch-logs-button > button {
                        background: linear-gradient(90deg, #12133f, #2A2B5A);
                        color: #FFFFFF;
                        border: none;
                        border-radius: 8px;
                        padding: 10px 20px;
                        font-size: 1em;
                        cursor: pointer;
                        transition: transform 0.2s;
                        margin: 10px auto;
                        display: block;
                    }
                    .fetch-logs-button > button:hover {
                        transform: scale(1.05);
                    }
                    .stDataFrame {
                        width: 100%;
                    }
                    .stDataFrame table {
                        width: 100%;
                        border-collapse: collapse;
                        background: #f0ede6;
                        font-family: 'Inter', sans-serif;
                    }
                    .stDataFrame th {
                        background: #F7F9FC;
                        color: #12133f;
                        font-weight: 500;
                        padding: 10px;
                        border-bottom: 2px solid #D1D5DB;
                        position: sticky;
                        top: 0;
                        z-index: 10;
                    }
                    .stDataFrame td {
                        padding: 10px;
                        border-bottom: 1px solid #EDEFF5;
                        color: #1A1A1A;
                        vertical-align: top;
                    }
                    .pagination-container {
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        margin: 15px 0;
                        gap: 15px;
                    }
                    .pagination-container select {
                        padding: 8px;
                        border-radius: 6px;
                        border: 1px solid #D1D5DB;
                        background: #f0ede6;
                        color: #1A1A1A;
                        font-size: 1em;
                    }
                    .control-buttons {
                        display: flex;
                        justify-content: center;
                        gap: 15px;
                        margin-top: 15px;
                    }
                    .control-buttons button {
                        padding: 10px 20px;
                        border-radius: 6px;
                        font-size: 1em;
                        cursor: pointer;
                    }
                    .download-button > button {
                        background: #38A169;
                        color: #FFFFFF;
                        border: none;
                        border-radius: 6px;
                        padding: 10px 20px;
                        font-size: 1em;
                        cursor: pointer;
                        transition: background 0.2s;
                    }
                    .download-button > button:hover {
                        background: #2F855A;
                    }
                    </style>
                    """,
                    unsafe_allow_html=True
                )
                if st.button(
                    "Fetch Logs",
                    key=f"{table_type}_fetch",
                    type="primary",
                    help="Fetch logs for the selected class/service and level",
                    args={"class": "fetch-logs-button"}
                ):
                    self._load_logs(
                        st.session_state[f"{table_type}_select"], 
                        st.session_state[f"{table_type}_level_select"], 
                        table_type, 
                        log_processor, 
                        page=1
                    )
                
                try:
                    if logs_to_display:
                        logger.debug(f"Rendering {len(logs_to_display)} logs for {table_type}")
                        with st.container():
                            st.subheader(f"{selected_level} Logs for {table_type.capitalize()} {selected_index}")
                            log_df = pd.DataFrame(logs_to_display)
                            logger.debug(f"Created DataFrame with {len(log_df)} rows")
                            
                            logs_per_page = st.session_state.logs_per_page
                            total_filtered_pages = (total_logs + logs_per_page - 1) // logs_per_page
                            
                            col1, col2 = st.columns([3, 1])
                            with col1:
                                search_query = st.text_input(
                                    "Search Logs (Message or Timestamp)",
                                    key=f"search_{table_type}",
                                    placeholder="Enter keyword or regex..."
                                )
                            with col2:
                                # Sync checkbox with reset state
                                regex_key = f"regex_{table_type}"
                                reset_key = f"reset_regex_{table_type}"
                                regex_value = False if st.session_state.get(reset_key, False) else st.session_state.get(regex_key, False)
                                st.checkbox(
                                    "Use Regex",
                                    key=regex_key,
                                    value=regex_value
                                )
                            
                            if search_query != st.session_state.get(f'search_query_{table_type}', ''):
                                st.session_state[f'search_query_{table_type}'] = search_query
                                st.session_state.log_page = 1
                                self._load_logs(
                                    selected_index, 
                                    selected_level, 
                                    table_type, 
                                    log_processor, 
                                    page=1, 
                                    search_query=search_query, 
                                    use_regex=st.session_state.get(regex_key, False)
                                )
                                log_df = pd.DataFrame(logs_to_display)
                                total_logs = (st.session_state.total_logs_class if table_type == 'class'
                                              else st.session_state.total_logs_service)
                                total_filtered_pages = (total_logs + logs_per_page - 1) // logs_per_page
                            
                            if st.session_state.log_page > total_filtered_pages:
                                st.session_state.log_page = max(1, total_filtered_pages)
                            
                            with st.container():
                                st.markdown('<div class="pagination-container">', unsafe_allow_html=True)
                                col1, col2, col3 = st.columns([1, 2, 1])
                                with col1:
                                    if st.button("Previous", key=f"prev_{table_type}", disabled=st.session_state.log_page <= 1):
                                        st.session_state.log_page = max(1, st.session_state.log_page - 1)
                                        self._load_logs(
                                            selected_index, 
                                            selected_level, 
                                            table_type, 
                                            log_processor, 
                                            page=st.session_state.log_page, 
                                            search_query=search_query, 
                                            use_regex=st.session_state.get(regex_key, False)
                                        )
                                        logger.debug(f"Navigated to page {st.session_state.log_page}")
                                with col2:
                                    page_options = list(range(1, total_filtered_pages + 1))
                                    selected_page = st.selectbox(
                                        "Select Page",
                                        options=page_options,
                                        index=st.session_state.log_page - 1,
                                        key=f"page_select_{table_type}",
                                        label_visibility="collapsed"
                                    )
                                    if selected_page != st.session_state.log_page:
                                        st.session_state.log_page = selected_page
                                        self._load_logs(
                                            selected_index, 
                                            selected_level, 
                                            table_type, 
                                            log_processor, 
                                            page=st.session_state.log_page, 
                                            search_query=search_query, 
                                            use_regex=st.session_state.get(regex_key, False)
                                        )
                                        logger.debug(f"Selected page {st.session_state.log_page}")
                                    st.markdown(
                                        f'<p style="text-align: center; color: #12133f;">Page {st.session_state.log_page} of {total_filtered_pages} ({total_logs} logs)</p>',
                                        unsafe_allow_html=True
                                    )
                                with col3:
                                    if st.button("Next", key=f"next_{table_type}", disabled=st.session_state.log_page >= total_filtered_pages):
                                        st.session_state.log_page = min(total_filtered_pages, st.session_state.log_page + 1)
                                        self._load_logs(
                                            selected_index, 
                                            selected_level, 
                                            table_type, 
                                            log_processor, 
                                            page=st.session_state.log_page, 
                                            search_query=search_query, 
                                            use_regex=st.session_state.get(regex_key, False)
                                        )
                                        logger.debug(f"Navigated to page {st.session_state.log_page}")
                                st.markdown('</div>', unsafe_allow_html=True)
                            
                            try:
                                st.dataframe(
                                    log_df,
                                    use_container_width=True,
                                    height=400,
                                    column_config={
                                        "Timestamp": st.column_config.TextColumn("Timestamp", width="medium"),
                                        "Log Message": st.column_config.TextColumn("Log Message", width="large"),
                                        "Level": st.column_config.TextColumn("Level", width="small"),
                                        "Class": st.column_config.TextColumn("Class", width="medium")
                                    }
                                )
                                logger.debug(f"Displayed {len(log_df)} logs on page {st.session_state.log_page} for {table_type}")
                            except Exception as e:
                                logger.error(f"Failed to render st.dataframe: {str(e)}\n{traceback.format_exc()}")
                                st.error(f"Error rendering logs: {str(e)}")
                            
                            try:
                                st.markdown('<div class="control-buttons">', unsafe_allow_html=True)
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.download_button(
                                        label="Download All Logs (JSON)",
                                        data=json.dumps(logs_to_display),
                                        file_name=f"{selected_level}_Logs_{table_type}_{selected_index}.json",
                                        mime="application/json",
                                        key=f"download_{table_type}",
                                        args={"class": "download-button"}
                                    )
                                with col2:
                                    if st.button("Close Logs", key=f"close_{table_type}", type="secondary"):
                                        self._clear_logs(table_type)
                                        logger.debug(f"Cleared logs for {table_type}")
                                st.markdown('</div>', unsafe_allow_html=True)
                            except Exception as e:
                                logger.error(f"Error rendering control buttons for {table_type}: {str(e)}\n{traceback.format_exc()}")
                                st.error(f"Error rendering control buttons: {str(e)}")
                    elif selected_index and selected_level:
                        st.info(f"No logs loaded for {table_type} {selected_index} and level {selected_level}. Click 'Fetch Logs' to load.")
                        logger.debug(f"No logs loaded for {table_type} {selected_index} {selected_level}")
                except Exception as e:
                    logger.error(f"Error rendering log form for {table_type}: {str(e)}\n{traceback.format_exc()}")
                    st.error(f"Error processing log form: {str(e)}")

    def _load_logs(self, index_col, level, table_type, log_processor, page=1, search_query=None, use_regex=False):
        """Load logs for the selected class/service and level for the specified page."""
        try:
            logger.debug(f"Loading logs for {table_type}={index_col}, level={level}, page={page}")
            with st.spinner("Loading logs..."):
                start_time = time.time()
                if table_type == 'class':
                    logs, total_logs = log_processor.get_logs_by_class_and_level(
                        index_col, level, page, st.session_state.logs_per_page, search_query, use_regex
                    )
                    st.session_state.logs_to_display_class = logs
                    st.session_state.logs_to_display_service = []
                    st.session_state.total_logs_class = total_logs
                    st.session_state.total_logs_service = 0
                    st.session_state.selected_class = index_col
                    st.session_state.selected_class_level = level
                else:
                    logs, total_logs = log_processor.get_logs_by_service_and_level(
                        index_col, level, page, st.session_state.logs_per_page, search_query, use_regex
                    )
                    st.session_state.logs_to_display_service = logs
                    st.session_state.logs_to_display_class = []
                    st.session_state.total_logs_service = total_logs
                    st.session_state.total_logs_class = 0
                    st.session_state.selected_service = index_col
                    st.session_state.selected_service_level = level
                logger.debug(f"Log loading took {time.time() - start_time:.2f} seconds")
                if not logs:
                    st.session_state.notifications.append({
                        'type': 'info',
                        'message': f"No valid logs found for {table_type} {index_col} and level {level}.",
                        'timestamp': time.time()
                    })
                    logger.info(f"No valid logs found for {table_type} {index_col} and level {level}")
                else:
                    logger.info(f"Loaded {len(logs)} logs for {table_type} {index_col} and level {level}, page {page}, total {total_logs}")
                st.rerun()
        except Exception as e:
            st.session_state.notifications.append({
                'type': 'error',
                'message': f"Error loading logs: {str(e)}",
                'timestamp': time.time()
            })
            logger.error(f"Error loading logs for {table_type} {index_col} and level {level}: {str(e)}\n{traceback.format_exc()}")

    def _clear_logs(self, table_type):
        """Clear displayed logs and reset state for the specific table type."""
        try:
            logger.debug(f"Clearing logs for {table_type}")
            if table_type == 'class':
                st.session_state.logs_to_display_class = []
                st.session_state.total_logs_class = 0
                st.session_state.selected_class = None
                st.session_state.selected_class_level = None
                st.session_state.search_query_class = ''
                st.session_state.reset_regex_class = True  # Signal to reset checkbox
            else:
                st.session_state.logs_to_display_service = []
                st.session_state.total_logs_service = 0
                st.session_state.selected_service = None
                st.session_state.selected_service_level = None
                st.session_state.search_query_service = ''
                st.session_state.reset_regex_service = True  # Signal to reset checkbox
            st.session_state.log_page = 1
            logger.debug(f"Cleared logs and state for {table_type}")
            st.rerun()  # Force refresh to update UI
        except Exception as e:
            logger.error(f"Error clearing logs for {table_type}: {str(e)}\n{traceback.format_exc()}")
            st.error(f"Error clearing logs: {str(e)}")
            st.rerun()

    def display_dashboard(self, level_counts_by_class, level_counts_by_service, timeline_data, class_service_counts, log_processor=None):
        """Display the complete visualization dashboard."""
        logger.info("Starting display_dashboard")
        try:
            with st.container():
                st.header("Analysis Dashboard")

                # Timeline Graph
                try:
                    st.subheader("Log Levels Timeline")
                    if not timeline_data.empty:
                        fig = self._create_timeline_figure(timeline_data)
                        st.plotly_chart(fig, use_container_width=True)
                        logger.debug("Displayed timeline graph")
                    else:
                        st.warning("No timeline data available. Ensure logs contain valid 'timestamp' fields.")
                        logger.warning("No data for timeline graph")
                except Exception as e:
                    logger.error(f"Error rendering timeline: {str(e)}\n{traceback.format_exc()}")
                    st.error(f"Error rendering timeline: {str(e)}")

            # Log Level Counts Tables and Log Forms
            with st.container():
                try:
                    st.subheader("Log Level Counts by Class")
                    if not level_counts_by_class.empty and level_counts_by_class.iloc[:, 1:].sum().sum() > 0:
                        st.dataframe(level_counts_by_class, use_container_width=True)
                        self._render_log_form(level_counts_by_class, 'class', log_processor)
                        logger.debug("Displayed log level counts and form for class")
                    else:
                        st.warning("No log level counts by class available. Ensure logs contain valid 'timestamp' and 'class' fields.")
                        logger.warning("No data for log level counts by class")
                except Exception as e:
                    logger.error(f"Error rendering class log counts: {str(e)}\n{traceback.format_exc()}")
                    st.error(f"Error rendering class log counts: {str(e)}")

            with st.container():
                try:
                    st.subheader("Log Level Counts by Service")
                    if not level_counts_by_service.empty and level_counts_by_service.iloc[:, 1:].sum().sum() > 0:
                        st.dataframe(level_counts_by_service, use_container_width=True)
                        self._render_log_form(level_counts_by_service, 'service', log_processor)
                        logger.debug("Displayed log level counts and form for service")
                    else:
                        st.warning("No log level counts by service available. Ensure logs contain valid 'timestamp' and 'service' fields.")
                        logger.warning("No data for log level counts by service")
                except Exception as e:
                    logger.error(f"Error rendering service log counts: {str(e)}\n{traceback.format_exc()}")
                    st.error(f"Error rendering service log counts: {str(e)}")

            # Pie Charts
            with st.container():
                try:
                    st.subheader("Logs by Class")
                    if not class_service_counts.empty:
                        class_counts = class_service_counts.groupby('class')['count'].sum().reset_index()
                        fig = self._create_pie_figure(class_counts, 'class', 'count', "Distribution by Class")
                        st.plotly_chart(fig, use_container_width=True)
                        logger.debug("Displayed pie chart for class distribution")
                    else:
                        st.warning("No class distribution data available. Ensure logs contain valid 'class' fields.")
                        logger.warning("No data for class pie chart")
                except Exception as e:
                    logger.error(f"Error rendering class pie chart: {str(e)}\n{traceback.format_exc()}")
                    st.error(f"Error rendering class pie chart: {str(e)}")

            with st.container():
                try:
                    st.subheader("Logs by Service")
                    if not class_service_counts.empty:
                        service_counts = class_service_counts.groupby('service')['count'].sum().reset_index()
                        fig = self._create_pie_figure(service_counts, 'service', 'count', "Distribution by Service")
                        st.plotly_chart(fig, use_container_width=True)
                        logger.debug("Displayed pie chart for service distribution")
                    else:
                        st.warning("No service distribution data available. Ensure logs contain valid 'service' fields.")
                        logger.warning("No data for service pie chart")
                except Exception as e:
                    logger.error(f"Error rendering service pie chart: {str(e)}\n{traceback.format_exc()}")
                    st.error(f"Error rendering service pie chart: {str(e)}")

            # Detailed Breakdown by Log Level
            with st.container():
                try:
                    st.subheader("Detailed Breakdown by Log Level")
                    for level in self.config['app']['log_levels']:
                        with st.expander(f"{level} Logs", expanded=False):
                            col1, col2 = st.columns(2)
                            with col1:
                                st.write("By Class")
                                if level in level_counts_by_class.columns:
                                    class_data = level_counts_by_class[['class', level]].copy()
                                    class_data = class_data[class_data[level] > 0].rename(columns={level: 'count'})
                                    if not class_data.empty:
                                        st.dataframe(class_data, use_container_width=True)
                                        logger.debug(f"Displayed {level} logs by class")
                                    else:
                                        st.info(f"No {level} logs by class")
                                        logger.debug(f"No data for {level} logs by class")
                                else:
                                    st.info(f"No {level} logs by class (level not present in data)")
                                    logger.debug(f"Level {level} not in level_counts_by_class columns")
                            with col2:
                                st.write("By Service")
                                if level in level_counts_by_service.columns:
                                    service_data = level_counts_by_service[['service', level]].copy()
                                    service_data = service_data[service_data[level] > 0].rename(columns={level: 'count'})
                                    if not service_data.empty:
                                        st.dataframe(service_data, use_container_width=True)
                                        logger.debug(f"Displayed {level} logs by service")
                                    else:
                                        st.info(f"No {level} logs by service")
                                        logger.debug(f"No data for {level} logs by service")
                                else:
                                    st.info(f"No {level} logs by service (level not present in data)")
                                    logger.debug(f"Level {level} not in level_counts_by_service columns")
                except Exception as e:
                    logger.error(f"Error rendering detailed breakdown: {str(e)}\n{traceback.format_exc()}")
                    st.error(f"Error rendering detailed breakdown: {str(e)}")
            logger.info("Completed display_dashboard")
        except Exception as e:
            logger.error(f"Error displaying dashboard: {str(e)}\n{traceback.format_exc()}")
            st.error(f"Error rendering visualizations: {str(e)}")