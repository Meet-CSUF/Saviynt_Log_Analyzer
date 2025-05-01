import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import logging
import json
import time
import traceback
from datetime import datetime

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
            'FATAL': '#805AD5',
            'JOB': '#FF6B6B',
            'NEW': '#4ECDC4',
            'TEST': '#45B7D1',
            'TSAP': '#96CEB4',
            'UNKNOWN': '#D4A5A5'
        }
        # Expected analysis names
        self.expected_analyses = [
            'class_level_counts',
            'level_summary',
            'class_summary',
            'pod_summary',
            'container_summary',
            'host_summary',
            'class_level_pod',
            'hourly_level_counts',
            'thread_summary',
            'error_analysis',
            'time_range'
        ]
        # Valid log levels for class_level_counts and class_level_pod
        self.valid_log_levels = ['DEBUG', 'ERROR', 'INFO', 'JOB', 'NEW', 'TEST', 'TSAP', 'UNKNOWN', 'WARN', 'FATAL']
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
    def _create_timeline_figure(_self, data, x, y, color=None, title=None):
        """Create cached timeline figure for single or multi-series data."""
        if color:
            fig = px.line(
                data,
                x=x,
                y=y,
                color=color,
                title=title,
                color_discrete_map=_self.colors
            )
        else:
            fig = px.line(
                data,
                x=x,
                y=y,
                title=title,
                color_discrete_sequence=['#12133f']
            )
        fig.update_layout(
            xaxis_title="Hour",
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

    @st.cache_data
    def _create_bar_figure(_self, data, x, y, color=None, title=None):
        """Create cached bar figure."""
        fig = px.bar(
            data,
            x=x,
            y=y,
            color=color,
            title=title,
            color_discrete_map=_self.colors if color in _self.colors else None
        )
        fig.update_layout(
            xaxis=dict(tickangle=45),
            plot_bgcolor='#f0ede6',
            paper_bgcolor='#f0ede6',
            font_color='#1A1A1A',
            title_font_color='#12133f',
            legend_title_font_color='#1A1A1A',
            font_size=14,
            height=400,
            margin=dict(t=50, b=100, l=50, r=50)
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
                        font-family: 'Inter', sans-serif;
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
                st.session_state.reset_regex_class = True
            else:
                st.session_state.logs_to_display_service = []
                st.session_state.total_logs_service = 0
                st.session_state.selected_service = None
                st.session_state.selected_service_level = None
                st.session_state.search_query_service = ''
                st.session_state.reset_regex_service = True
            st.session_state.log_page = 1
            logger.debug(f"Cleared logs and state for {table_type}")
            st.rerun()
        except Exception as e:
            logger.error(f"Error clearing logs for {table_type}: {str(e)}\n{traceback.format_exc()}")
            st.error(f"Error clearing logs: {str(e)}")
            st.rerun()

    def display_dashboard(self, level_counts_by_class, level_counts_by_service, timeline_data, class_service_counts, log_processor=None):
        """Display the complete visualization dashboard for log analysis."""
        logger.info("Starting display_dashboard")
        try:
            with st.container():
                st.header("Analysis Dashboard")

                # Timeline Graph
                try:
                    st.subheader("Log Levels Timeline")
                    if not timeline_data.empty:
                        fig = self._create_timeline_figure(timeline_data, 'timestamp', 'count', 'level', "Log Levels Over Time")
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

    def display_csv_dashboard(self, csv_data):
        """Display a comprehensive dashboard for uploaded CSV data."""
        logger.info("Starting display_csv_dashboard")
        try:
            with st.container():
                st.header("CSV Data Visualization Dashboard")

                # Time Range
                if 'time_range' in csv_data and not csv_data['time_range'].empty:
                    st.subheader("Time Range Analysis")
                    try:
                        time_range = csv_data['time_range']
                        required_columns = ['start_time', 'end_time']
                        if not all(col in time_range.columns for col in required_columns):
                            st.warning("Time range CSV missing required columns: start_time, end_time.")
                            logger.error("Time range CSV missing required columns")
                        else:
                            start_time = pd.to_datetime(time_range['start_time'].iloc[0])
                            end_time = pd.to_datetime(time_range['end_time'].iloc[0])
                            duration_hours = (end_time - start_time).total_seconds() / 3600
                            st.write(f"**Start Time:** {start_time}")
                            st.write(f"**End Time:** {end_time}")
                            st.write(f"**Duration (Hours):** {duration_hours:.2f}")
                            logger.debug("Displayed time range analysis")
                    except Exception as e:
                        st.warning("Error processing time range data. Ensure valid datetime format in start_time and end_time.")
                        logger.error(f"Error displaying time_range: {str(e)}")
                else:
                    st.warning("Time range analysis not available. Upload time_range_YYYYMMDD_HHMMSS.csv.")
                    logger.debug("No time_range data available")

                # Log Level Distribution
                if 'level_summary' in csv_data and not csv_data['level_summary'].empty:
                    st.subheader("Log Level Distribution")
                    try:
                        level_summary = csv_data['level_summary']
                        required_columns = ['level', 'count']
                        if not all(col in level_summary.columns for col in required_columns):
                            st.warning("Level summary CSV missing required columns: level, count.")
                            logger.error("Level summary CSV missing required columns")
                        else:
                            fig = self._create_pie_figure(level_summary, 'level', 'count', "Log Level Distribution")
                            st.plotly_chart(fig, use_container_width=True)
                            st.dataframe(level_summary, use_container_width=True)
                            logger.debug("Displayed log level distribution")
                    except Exception as e:
                        st.warning("Error displaying log level distribution. Ensure level_summary.csv has valid data.")
                        logger.error(f"Error displaying level_summary: {str(e)}")
                else:
                    st.warning("Log level distribution not available. Upload level_summary_YYYYMMDD_HHMMSS.csv.")
                    logger.debug("No level_summary data available")

                # Class-Level Pie Charts
                if 'class_level_counts' in csv_data and not csv_data['class_level_counts'].empty:
                    st.subheader("Log Levels by Class")
                    try:
                        class_level = csv_data['class_level_counts']
                        required_columns = ['class']
                        available_levels = [col for col in class_level.columns if col in self.valid_log_levels]
                        if 'class' not in class_level.columns:
                            st.warning("Class level counts CSV missing required column: class.")
                            logger.error("Class level counts CSV missing column: class")
                        elif not available_levels:
                            st.warning(f"Class level counts CSV has no recognized log level columns (expected: {', '.join(self.valid_log_levels)}).")
                            logger.error("Class level counts CSV has no recognized log level columns")
                        else:
                            logger.debug(f"Found log levels for class_level_counts: {available_levels}")
                            st.info(f"Using available log levels: {', '.join(available_levels)}.")
                            # Create summary DataFrame
                            summary_df = class_level[['class'] + available_levels].copy()
                            summary_df['Total'] = summary_df[available_levels].sum(axis=1)
                            for level in available_levels:
                                with st.expander(f"{level} Logs by Class", expanded=False):
                                    level_data = class_level[['class', level]].copy()
                                    level_data = level_data[level_data[level] > 0].rename(columns={level: 'Count'})
                                    if not level_data.empty:
                                        fig = self._create_pie_figure(
                                            level_data,
                                            'class',
                                            'Count',
                                            f"{level} Log Distribution by Class"
                                        )
                                        st.plotly_chart(fig, use_container_width=True)
                                        st.dataframe(level_data, use_container_width=True)
                                        logger.debug(f"Displayed {level} pie chart for class_level_counts")
                                    else:
                                        st.info(f"No {level} logs found for any class.")
                                        logger.debug(f"No data for {level} in class_level_counts")
                            st.subheader("Class Level Counts Summary")
                            st.dataframe(summary_df, use_container_width=True)
                            logger.debug("Displayed class level counts summary DataFrame")
                    except Exception as e:
                        st.warning("Error displaying log levels by class. Ensure class_level_counts.csv has valid data.")
                        logger.error(f"Error displaying class_level_counts: {str(e)}")
                else:
                    st.warning("Log levels by class not available. Upload class_level_counts_YYYYMMDD_HHMMSS.csv.")
                    logger.debug("No class_level_counts data available")

                # Summary of loaded analyses
                loaded_analyses = list(csv_data.keys())
                st.subheader("Loaded Analyses")
                if loaded_analyses:
                    st.write(f"Loaded {len(loaded_analyses)} analyses: {', '.join(loaded_analyses)}")
                    missing_analyses = [a for a in self.expected_analyses if a not in loaded_analyses]
                    if missing_analyses:
                        st.warning(f"Missing analyses: {', '.join(missing_analyses)}. Upload corresponding CSV files (e.g., {missing_analyses[0]}_YYYYMMDD_HHMMSS.csv).")
                    logger.debug(f"Loaded analyses: {loaded_analyses}, missing: {missing_analyses}")
                else:
                    st.warning("No analyses loaded. Upload CSV files with names like time_range_YYYYMMDD_HHMMSS.csv.")
                    logger.warning("No analyses loaded for CSV dashboard")
                    return

                # Class Summary
                if 'class_summary' in csv_data and not csv_data['class_summary'].empty:
                    st.subheader("Class Distribution")
                    try:
                        class_summary = csv_data['class_summary']
                        required_columns = ['class', 'count']
                        if not all(col in class_summary.columns for col in required_columns):
                            st.warning("Class summary CSV missing required columns: class, count.")
                            logger.error("Class summary CSV missing required columns")
                        else:
                            fig = self._create_bar_figure(class_summary.head(10), 'class', 'count', title="Top 10 Classes by Log Count")
                            st.plotly_chart(fig, use_container_width=True)
                            st.dataframe(class_summary, use_container_width=True)
                            logger.debug("Displayed class distribution")
                    except Exception as e:
                        st.warning("Error displaying class distribution. Ensure class_summary.csv has valid data.")
                        logger.error(f"Error displaying class_summary: {str(e)}")
                else:
                    st.warning("Class distribution not available. Upload class_summary_YYYYMMDD_HHMMSS.csv.")
                    logger.debug("No class_summary data available")

                # Pod Summary
                if 'pod_summary' in csv_data and not csv_data['pod_summary'].empty:
                    st.subheader("Pod Distribution")
                    try:
                        pod_summary = csv_data['pod_summary']
                        required_columns = ['pod', 'count']
                        if not all(col in pod_summary.columns for col in required_columns):
                            st.warning("Pod summary CSV missing required columns: pod, count.")
                            logger.error("Pod summary CSV missing required columns")
                        else:
                            fig = self._create_bar_figure(pod_summary.head(10), 'pod', 'count', title="Top 10 Pods by Log Count")
                            st.plotly_chart(fig, use_container_width=True)
                            st.dataframe(pod_summary, use_container_width=True)
                            logger.debug("Displayed pod distribution")
                    except Exception as e:
                        st.warning("Error displaying pod distribution. Ensure pod_summary.csv has valid data.")
                        logger.error(f"Error displaying pod_summary: {str(e)}")
                else:
                    st.warning("Pod distribution not available. Upload pod_summary_YYYYMMDD_HHMMSS.csv.")
                    logger.debug("No pod_summary data available")

                # Container Summary
                if 'container_summary' in csv_data and not csv_data['container_summary'].empty:
                    st.subheader("Container Distribution")
                    try:
                        container_summary = csv_data['container_summary']
                        required_columns = ['container', 'count']
                        if not all(col in container_summary.columns for col in required_columns):
                            st.warning("Container summary CSV missing required columns: container, count.")
                            logger.error("Container summary CSV missing required columns")
                        else:
                            fig = self._create_bar_figure(container_summary.head(10), 'container', 'count', title="Top 10 Containers by Log Count")
                            st.plotly_chart(fig, use_container_width=True)
                            st.dataframe(container_summary, use_container_width=True)
                            logger.debug("Displayed container distribution")
                    except Exception as e:
                        st.warning("Error displaying container distribution. Ensure container_summary.csv has valid data.")
                        logger.error(f"Error displaying container_summary: {str(e)}")
                else:
                    st.warning("Container distribution not available. Upload container_summary_YYYYMMDD_HHMMSS.csv.")
                    logger.debug("No container_summary data available")

                # Host Summary
                if 'host_summary' in csv_data and not csv_data['host_summary'].empty:
                    st.subheader("Host Distribution")
                    try:
                        host_summary = csv_data['host_summary']
                        required_columns = ['host', 'count']
                        if not all(col in host_summary.columns for col in required_columns):
                            st.warning("Host summary CSV missing required columns: host, count.")
                            logger.error("Host summary CSV missing required columns")
                        else:
                            fig = self._create_bar_figure(host_summary.head(10), 'host', 'count', title="Top 10 Hosts by Log Count")
                            st.plotly_chart(fig, use_container_width=True)
                            st.dataframe(host_summary, use_container_width=True)
                            logger.debug("Displayed host distribution")
                    except Exception as e:
                        st.warning("Error displaying host distribution. Ensure host_summary.csv has valid data.")
                        logger.error(f"Error displaying host_summary: {str(e)}")
                else:
                    st.warning("Host distribution not available. Upload host_summary_YYYYMMDD_HHMMSS.csv.")
                    logger.debug("No host_summary data available")

                # Thread Summary
                if 'thread_summary' in csv_data and not csv_data['thread_summary'].empty:
                    st.subheader("Thread Distribution")
                    try:
                        thread_summary = csv_data['thread_summary']
                        required_columns = ['thread', 'count']
                        if not all(col in thread_summary.columns for col in required_columns):
                            st.warning("Thread summary CSV missing required columns: thread, count.")
                            logger.error("Thread summary CSV missing required columns")
                        else:
                            fig = self._create_bar_figure(thread_summary.head(10), 'thread', 'count', title="Top 10 Threads by Log Count")
                            st.plotly_chart(fig, use_container_width=True)
                            st.dataframe(thread_summary, use_container_width=True)
                            logger.debug("Displayed thread distribution")
                    except Exception as e:
                        st.warning("Error displaying thread distribution. Ensure thread_summary.csv has valid data.")
                        logger.error(f"Error displaying thread_summary: {str(e)}")
                else:
                    st.warning("Thread distribution not available. Upload thread_summary_YYYYMMDD_HHMMSS.csv.")
                    logger.debug("No thread_summary data available")

                # Hourly Log Level Counts
                if 'hourly_level_counts' in csv_data and not csv_data['hourly_level_counts'].empty:
                    st.subheader("Hourly Log Counts")
                    try:
                        hourly_counts = csv_data['hourly_level_counts']
                        required_columns = ['hour', 'count']
                        if not all(col in hourly_counts.columns for col in required_columns):
                            missing_cols = [col for col in required_columns if col not in hourly_counts.columns]
                            st.warning(f"Hourly log counts CSV missing required columns: {', '.join(missing_cols)}.")
                            logger.error(f"Hourly log counts CSV missing columns: {missing_cols}")
                        else:
                            hourly_counts = hourly_counts.sort_values('hour')
                            hourly_counts['hour'] = hourly_counts['hour'].astype(float)
                            fig = self._create_timeline_figure(
                                hourly_counts,
                                'hour',
                                'count',
                                None,
                                "Hourly Log Count Trends"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                            st.dataframe(hourly_counts, use_container_width=True)
                            logger.debug("Displayed hourly log count timeline")
                    except Exception as e:
                        st.warning("Error displaying hourly log counts. Ensure hourly_level_counts.csv has valid data.")
                        logger.error(f"Error displaying hourly_level_counts: {str(e)}")
                else:
                    st.warning("Hourly log counts not available. Upload hourly_level_counts_YYYYMMDD_HHMMSS.csv.")
                    logger.debug("No hourly_level_counts data available")

                # Class-Pod-Level Pie Charts
                if 'class_level_pod' in csv_data and not csv_data['class_level_pod'].empty:
                    st.subheader("Log Levels by Class and Pod")
                    try:
                        class_level_pod = csv_data['class_level_pod']
                        required_columns = ['class', 'pod']
                        available_levels = [col for col in class_level_pod.columns if col in self.valid_log_levels]
                        if not all(col in class_level_pod.columns for col in required_columns):
                            missing_cols = [col for col in required_columns if col not in class_level_pod.columns]
                            st.warning(f"Class level pod CSV missing required columns: {', '.join(missing_cols)}.")
                            logger.error(f"Class level pod CSV missing required columns: {missing_cols}")
                        elif not available_levels:
                            st.warning(f"Class level pod CSV has no recognized log level columns (expected: {', '.join(self.valid_log_levels)}).")
                            logger.error("Class level pod CSV has no recognized log level columns")
                        else:
                            logger.debug(f"Found log levels for class_level_pod: {available_levels}")
                            st.info(f"Using available log levels: {', '.join(available_levels)}.")
                            # Create summary DataFrame
                            summary_df = class_level_pod[['class', 'pod'] + available_levels].copy()
                            summary_df['Total'] = summary_df[available_levels].sum(axis=1)
                            for level in available_levels:
                                with st.expander(f"{level} Logs by Class and Pod", expanded=False):
                                    level_data = class_level_pod[['class', 'pod', level]].copy()
                                    level_data['Class_Pod'] = level_data['class'] + ' / ' + level_data['pod']
                                    level_data = level_data[level_data[level] > 0][['Class_Pod', level]].rename(columns={level: 'Count'})
                                    if not level_data.empty:
                                        fig = self._create_pie_figure(
                                            level_data,
                                            'Class_Pod',
                                            'Count',
                                            f"{level} Log Distribution by Class and Pod"
                                        )
                                        st.plotly_chart(fig, use_container_width=True)
                                        st.dataframe(level_data[['Class_Pod', 'Count']], use_container_width=True)
                                        logger.debug(f"Displayed {level} pie chart for class_level_pod")
                                    else:
                                        st.info(f"No {level} logs found for any class/pod combination.")
                                        logger.debug(f"No data for {level} in class_level_pod")
                            st.subheader("Class and Pod Level Counts Summary")
                            st.dataframe(summary_df, use_container_width=True)
                            logger.debug("Displayed class and pod level counts summary DataFrame")
                    except Exception as e:
                        st.warning("Error displaying log levels by class and pod. Ensure class_level_pod.csv has valid data.")
                        logger.error(f"Error displaying class_level_pod: {str(e)}")
                else:
                    st.warning("Log levels by class and pod not available. Upload class_level_pod_YYYYMMDD_HHMMSS.csv.")
                    logger.debug("No class_level_pod data available")

                # Error Analysis
                if 'error_analysis' in csv_data and not csv_data['error_analysis'].empty:
                    st.subheader("Error Analysis")
                    try:
                        error_analysis = csv_data['error_analysis']
                        required_columns = ['class', 'count']
                        if not all(col in error_analysis.columns for col in required_columns):
                            missing_cols = [col for col in required_columns if col not in error_analysis.columns]
                            st.warning(f"Error analysis CSV missing required columns: {', '.join(missing_cols)}.")
                            logger.error(f"Error analysis CSV missing required columns: {missing_cols}")
                        else:
                            # Group by class and sum counts
                            error_analysis = error_analysis.groupby('class')['count'].sum().reset_index()
                            fig = self._create_bar_figure(
                                error_analysis.head(10),
                                'class',
                                'count',
                                title="Top 10 Error Counts by Class"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                            st.dataframe(error_analysis[['class', 'count']], use_container_width=True)
                            logger.debug("Displayed error analysis")
                    except Exception as e:
                        st.warning("Error displaying error analysis. Ensure error_analysis.csv has valid data.")
                        logger.error(f"Error displaying error_analysis: {str(e)}")
                else:
                    st.warning("Error analysis not available. Upload error_analysis_YYYYMMDD_HHMMSS.csv.")
                    logger.debug("No error_analysis data available")

                logger.info("Completed display_csv_dashboard")
        except Exception as e:
            logger.error(f"Error displaying CSV dashboard: {str(e)}\n{traceback.format_exc()}")
            st.error(f"Error rendering CSV visualizations: {str(e)}")