import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
import pdfkit
from jinja2 import Template

def parse_call2action_data(json_data: str) -> List[Dict[str, Any]]:
    """
    Parse the call2action JSON data into a list of dictionaries.
    
    Args:
        json_data: String containing JSON data (one JSON object per line)
        
    Returns:
        List of parsed call2action items
    """
    items = []
    for line in json_data.strip().split('\n'):
        if line.strip():
            try:
                item = json.loads(line.strip())
                items.append(item)
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON line: {line}")
                print(f"Error: {e}")
                continue
    return items

def group_by_date_and_committee(items):
    """
    Group call2action items by date and then by committee.
    
    Args:
        items: List of call2action items
        
    Returns:
        Dictionary grouped by date -> committee -> list of items
    """
    grouped = {}
    
    for item in items:
        meeting_datetime = item.get('meeting_datetime', '')
        cmt = item.get('CMT', '')
        
        # Extract date from meeting_datetime (format: "07/22/2025 09:00 AM")
        try:
            date_part = meeting_datetime.split(' ')[0]
            date_obj = datetime.strptime(date_part, '%m/%d/%Y')
            date_key = date_obj.strftime('%A, %B %d, %Y')
        except (ValueError, IndexError):
            date_key = 'Unknown Date'
        
        # Extract committee info from CMT field
        committee_lines = cmt.split('\n')
        if len(committee_lines) >= 4:
            committee_time_location = f'{committee_lines[2]} - {committee_lines[3]}'
        else:
            committee_time_location = ''
        
        if len(committee_lines) >= 2:
            committee_key = f"{committee_lines[0]} - {committee_lines[1]}"
        else:
            committee_key = cmt or 'Unknown Committee'
        
        if date_key not in grouped:
            grouped[date_key] = {}
        
        if committee_key not in grouped[date_key]:
            grouped[date_key][committee_key] = []
        
        item['committee_time_location'] = committee_time_location
        grouped[date_key][committee_key].append(item)
    
    return grouped

def get_html_template() -> str:
    """
    Return the HTML template for the call2action PDF.
    """
    template_path = Path(__file__).parent / 'call2action_template.html'
    with open(template_path, 'r', encoding='utf-8') as f:
        return f.read()

def get_image_path() -> str:
    """
    Get the absolute path to the Texas Capitol image for the footer.
    Returns a file:// URL that works with pdfkit.
    """
    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    image_path = script_dir / 'images' / 'capitol_background.png'
    
    # Convert to absolute path and then to file:// URL
    absolute_path = image_path.resolve()
    
    # Check if image exists
    if not image_path.exists():
        print(f"Warning: Image not found at {image_path}")
        return ""
    
    # Convert to file:// URL format for pdfkit
    file_url = f"file://{absolute_path}"
    return file_url

def generate_pdf_from_call2action(json_data: str, output_filename: str = None) -> str:
    """
    Generate a PDF from call2action JSON data.
    
    Args:
        json_data: String containing JSON data (one JSON object per line)
        output_filename: Optional custom filename for the PDF
        
    Returns:
        Path to the generated PDF file
    """
    # Parse the data
    items = parse_call2action_data(json_data)
    
    if not items:
        raise ValueError("No valid call2action items found in the provided data")
    
    # Group the data
    grouped_data = group_by_date_and_committee(items)
    
    # Create output directory if it doesn't exist
    output_dir = Path("out")
    output_dir.mkdir(exist_ok=True)
    
    # Generate filename if not provided
    if output_filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"call2action_summary_{timestamp}.pdf"
    
    output_path = output_dir / output_filename
    print(output_path)
    
    # Get the image path for the footer
    footer_image_path = get_image_path()
    
    # Render HTML template
    template = Template(get_html_template())
    print(grouped_data)

    for date in grouped_data.keys():
        html_content = template.render(
            grouped_data=grouped_data[date],
            generation_date=datetime.now().strftime("%B %d, %Y at %I:%M %p"),
            date=date,
            background_image_path=footer_image_path
        )
        
        # Save HTML for debugging (optional)
        html_path = output_dir / f"{output_filename.replace('.pdf', '.html')}"
        print(html_path)
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"HTML template saved to: {html_path}")
    
    # # Configure pdfkit options
    # options = {
    #     'page-size': 'Letter',
    #     'margin-top': '0in',
    #     'margin-right': '0in',
    #     'margin-bottom': '0in',
    #     'margin-left': '0in',
    #     'encoding': "UTF-8",
    #     'no-outline': None,
    #     'enable-local-file-access': None
    # }
    
    # try:
    #     # Generate PDF
    #     pdfkit.from_string(html_content, str(output_path), options=options)
    #     print(f"PDF generated successfully: {output_path}")
    #     return str(output_path)
        
    # except Exception as e:
    #     print(f"Error generating PDF: {e}")
    #     print("Make sure wkhtmltopdf is installed on your system.")
    #     print("You can install it from: https://wkhtmltopdf.org/downloads.html")
    #     raise

def main():
    """
    Main function for command-line usage.
    """
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python call2action.py <json_data_file> [output_filename]")
        print("Example: python call2action.py data.jsonl call2action_report.pdf")
        return
    
    input_file = sys.argv[1]
    output_filename = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            json_data = f.read()
        
        pdf_path = generate_pdf_from_call2action(json_data, output_filename)
        print(f"PDF successfully generated: {pdf_path}")
        
    except FileNotFoundError:
        print(f"Error: Could not find input file '{input_file}'")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
