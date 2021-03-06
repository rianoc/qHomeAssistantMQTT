from jinja2 import Template
import json

def states(sensor):
    return None;

def extract(template, msg):
    template = Template(template)
    template.globals['states'] = states
    return template.render(value_json=json.loads(msg));
