import math

def compute_distance(shapes_dct):
    """
    Computes the Manhattan distance in kilometers between consecutive geographic points 
    for each shape in a given dictionary.

    This function calculates pairwise Manhattan distances (city block distance) 
    between latitude/longitude coordinate pairs for each shape identifier. 
    Distances are computed by converting geographic degree differences to kilometers, 
    accounting for the curvature of the Earth in longitude conversions.

    Parameters
    ----------
    shapes_dct : dict
        A dictionary where keys are `shape_id` values (typically strings or integers), 
        and values are lists of tuples representing geographic coordinates 
        in the format (latitude, longitude). Each list must contain at least two points.

    Returns
    -------
    dict
        A dictionary where each key is a `shape_id` and the corresponding value is 
        a list of Manhattan distances (in kilometers) between consecutive coordinate pairs.

    Notes
    -----
    - Latitude differences are converted using a constant approximation: 1 degree ≈ 111.32 km.
    - Longitude differences are converted using the same factor scaled by cos(latitude) to 
      account for Earth's curvature.
    - The distance is computed using the Manhattan method: `distance = Δlat_km + Δlon_km`.
    - The function excludes the first and last coordinate in each shape path when computing 
      distances, because it iterates from index 1 to len - 2.
    """
    
    all_distances = {}
    for shape in shapes_dct:
        distances = []
        for coordinate in range(1, len(shapes_dct[shape]) - 1):
            lat1, lon1 = shapes_dct[shape][coordinate]
            lat2, lon2 = shapes_dct[shape][coordinate + 1]

            # Compute absolute lat/lon differences
            d_lat = abs(lat2 - lat1)
            d_lon = abs(lon2 - lon1)
            avg_lat = (lat1 + lat2) / 2

            # Convert degree diffs to km
            lat_km = d_lat * 111.32
            lon_km = d_lon * 111.32 * math.cos(math.radians(avg_lat))

            distance_km = lat_km + lon_km  # Manhattan distance in km
            distances.append(distance_km)
        
        all_distances[shape] = distances
    return all_distances