import redis
import os
import json

ESRI_API_KEY = os.environ.get('ESRI_API_KEY')
redis_instance = redis.StrictRedis.from_url(
    os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
)

catgory_order = [
    'C-MAN WEATHER STATIONS',	
    'CLIMATE REFERENCE MOORED BUOYS',
    'DRIFTING BUOYS',
    'DRIFTING BUOYS (GENERIC)',
    'GLIDERS',
    'GLOSS',
    'ICE BUOYS',
    'MOORED BUOYS',
    'MOORED BUOYS (GENERIC)',
    'OCEAN TRANSPORT STATIONS (GENERIC)',
    'PROFILING FLOATS AND GLIDERS',
    'PROFILING FLOATS AND GLIDERS (GENERIC)',
    'RESEARCH',
    'SHIPS',
    'SHIPS (GENERIC)',
    'SHORE AND BOTTOM STATIONS (GENERIC)',
    'TAGGED ANIMAL',
    'TIDE GAUGE STATIONS (GENERIC)',
    'TROPICAL MOORED BUOYS',
]

all_variables = []
long_names = {}

with open('schema.json', "r") as fp:
    schema = json.load(fp)

for var in schema:
    all_variables.append(var['name'])
    long_names[var['name']] = var['description']

meta_variables = ['latitude', 'longitude', 'observation_date', 'observation_depth', 'lon360', 'platform_code', 'platform_type', 'country', 'platform_id', 'longitude360']

data_variables = [x for x in all_variables if (x not in meta_variables)]

# Surface variables
surface_variables = data_variables.copy()
surface_variables.remove('zsal')
surface_variables.remove('ztmp')

# Depth variables

depth_variables = ['ztmp', 'zsal']

theme = {
    "accent":"#1f78b4",
    "accent_positive":"#017500",
    "accent_negative":"#C20000",
    "background_content":"#F9F9F9",
    "background_page":"#F2F2F2",
    "body_text":"#606060",
    "border":"#e2e2e2",
    "border_style":{
        "name":"underlined",
        "borderWidth":"0px 0px 1px 0px",
        "borderStyle":"solid",
        "borderRadius":0
    },
    "button_border":{
        "width":"1px",
        "color":"#1f78b4",
        "radius":"0px"
    },
    "button_capitalization":"uppercase",
    "button_text":"#1f78b4",
    "button_background_color":"#F9F9F9",
    "control_border":{
        "width":"0px 0px 1px 0px",
        "color":"#e2e2e2",
        "radius":"0px"
    },
    "control_background_color":"#F9F9F9",
    "control_text":"#606060",
    "card_margin":0,
    "card_padding":"5px",
    "card_border":{
        "width":"1px",
        "style":"solid",
        "color":"#e2e2e2",
        "radius":"0px"
    },
    "card_background_color":"#F9F9F9",
    "card_box_shadow":"0px 0px 0px rgba(0,0,0,0)",
    "card_outline":{
        "width":"1px",
        "style":"solid",
        "color":"#e2e2e2"
    },
    "card_header_accent":"#e2e2e2",
    "card_header_margin":"0px",
    "card_header_padding":"10px",
    "card_header_border":{
        "width":"0px 0px 1px 0px",
        "style":"solid",
        "color":"#e2e2e2",
        "radius":"0px"
    },
    "card_header_background_color":"#F9F9F9",
    "card_title_text":"#606060",
    "card_title_font_size":"20px",
    "card_description_background_color":"#FFF",
    "card_description_text":"#101010",
    "card_description_font_size":"16px",
    "card_menu_background_color":"#FFF",
    "card_menu_text":"#101010",
    "card_header_box_shadow":"0px 0px 0px rgba(0,0,0,0)",
    "card_accent":"#1f78b4",
    "breakpoint_font":"1200px",
    "breakpoint_stack_blocks":"700px",
    "colorway":[
        "#119dff",
        "#66c2a5",
        "#fc8d62",
        "#e78ac3",
        "#a6d854",
        "#ffd92f",
        "#e5c494",
        "#b3b3b3"
    ],
    "colorscale":[
        "#1f78b4",
        "#4786bc",
        "#6394c5",
        "#7ba3cd",
        "#92b1d5",
        "#a9c0de",
        "#bed0e6",
        "#d4dfee",
        "#eaeff7",
        "#ffffff"
    ],
    "dbc_primary":"#1f78b4",
    "dbc_secondary":"#7c7c7c",
    "dbc_info":"#009AC7",
    "dbc_gray":"#adb5bd",
    "dbc_success":"#017500",
    "dbc_warning":"#F9F871",
    "dbc_danger":"#C20000",
    "font_family":"Open Sans",
    "font_family_header":"Open Sans",
    "font_family_headings":"Open Sans",
    "font_size":"17px",
    "font_size_smaller_screen":"15px",
    "font_size_header":"24px",
    "footer_background_color":"#DDD",
    "footer_title_text":"#262626",
    "footer_title_font_size":"24px",
    "title_capitalization":"uppercase",
    "header_content_alignment":"spread",
    "header_margin":"0px",
    "header_padding":"0px",
    "header_border":{
        "width":"0px 0px 1px 0px",
        "style":"solid",
        "color":"#e2e2e2",
        "radius":"0px"
    },
    "header_background_color":"#F2F2F2",
    "header_box_shadow":"none",
    "header_text":"#606060",
    "heading_text":"#606060",
    "hero_background_color":"#F9F9F9",
    "hero_title_text":"#474747",
    "hero_title_font_size":"48px",
    "hero_subtitle_text":"#606060",
    "hero_subtitle_font_size":"16px",
    "hero_controls_background_color":"rgba(230, 230, 230, 0.90)",
    "hero_controls_label_text":"#464646",
    "hero_controls_label_font_size":"14px",
    "hero_controls_grid_columns":4,
    "hero_controls_accent":"#c1c1c1",
    "hero_border":{
        "width":"0",
        "style":"solid",
        "color":"transparent"
    },
    "hero_padding":"24px",
    "hero_gap":"24px",
    "text":"#606060",
    "report_background":"#F2F2F2",
    "report_background_content":"#FAFBFC",
    "report_background_page":"white",
    "report_text":"black",
    "report_font_family":"Computer Modern",
    "report_font_size":"12px",
    "section_padding":"24px",
    "section_title_font_size":"24px",
    "section_gap":"24px",
    "report_border":"#e2e2e2",
    "graph_grid_color":"#e2e2e2",
    "table_striped_even":"rgba(255,255,255,0)",
    "table_striped_odd":"rgba(0,0,0,0.05)",
    "table_border":"#e2e2e2",
    "tag_background_color":"#F1F1F1",
    "tag_text":"#474747",
    "tag_font_size":"14px",
    "tag_border":{
        "width":"1px",
        "style":"solid",
        "color":"#BEBEBE",
        "radius":"0px"
    },
    "tooltip_background_color":"#253247",
    "tooltip_text":"#FFF",
    "tooltip_font_size":"14px",
    "top_control_panel_border":{
        "width":"1px",
        "style":"solid",
        "color":"#DDD"
    }
}
