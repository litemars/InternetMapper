import json
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature


def parse_graph_json(json_path):
    """
    Parse the graph JSON and return nodes and relationships.
    """
    with open(json_path, 'r') as f:
        data = json.load(f)
    nodes_by_id = {node['id']: node for node in data['nodes']}
    relationships = data['relationships']
    return nodes_by_id, relationships


def extract_coordinates(nodes_by_id):
    """
    Extract coordinates for all nodes with latitude and longitude.
    """
    coords = {}
    for node_id, node in nodes_by_id.items():
        props = node['properties']
        lat = props.get('latitude')
        lon = props.get('longitude')
        try:
            if lat is not None and lon is not None:
                lat_f = float(lat)
                lon_f = float(lon)
                # Only plot if in valid range
                if -90 <= lat_f <= 90 and -180 <= lon_f <= 180:
                    coords[node_id] = (lat_f, lon_f)
        except Exception:
            continue
    return coords


def plot_graph_on_world(json_path, output_path=None):
    """
    Plot the graph nodes and relationships on a world map, only where both nodes have coordinates.
    """
    nodes_by_id, relationships = parse_graph_json(json_path)
    coords = extract_coordinates(nodes_by_id)

    plt.figure(figsize=(14, 8))
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.set_global()
    ax.coastlines()
    ax.add_feature(cfeature.BORDERS, linestyle=':')
    ax.add_feature(cfeature.LAND, facecolor='lightgray')
    ax.add_feature(cfeature.OCEAN, facecolor='lightblue')

    # Plot only nodes with coordinates
    for node_id, (lat, lon) in coords.items():
        # Check if it's the last node in the relationships
        is_last_node = not any(rel['start'] == node_id for rel in relationships)
        if is_last_node:
            ax.plot(lon, lat, 'go', markersize=8, transform=ccrs.PlateCarree(), label='Last Node' if 'Last Node' not in ax.get_legend_handles_labels()[1] else "")
        else:
            ax.plot(lon, lat, 'ro', markersize=6, transform=ccrs.PlateCarree())

    # Plot only relationships where both nodes have coordinates
    for rel in relationships:
        start = rel['start']
        end = rel['end']
        if start in coords and end in coords:
            lat1, lon1 = coords[start]
            lat2, lon2 = coords[end]
            ax.plot([lon1, lon2], [lat1, lat2], 'b-', linewidth=2, transform=ccrs.PlateCarree())

    plt.title('Traceroute Graph on World Map')
    if output_path:
        plt.savefig(output_path, bbox_inches='tight')
    else:
        plt.show()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Plot traceroute graph on world map from JSON export.")
    parser.add_argument('json_path', help='Path to the exported graph JSON file')
    parser.add_argument('--output', help='Path to save the plot image (optional)')
    args = parser.parse_args()
    plot_graph_on_world(args.json_path, args.output)
