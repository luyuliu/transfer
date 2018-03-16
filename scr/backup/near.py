
import arcpy
print("Libary done")
# set workspace environment
arcpy.env.workspace = "I:\\OSU\\Transfer\\data\\workspace.gdb"
arcpy.env.overwriteOutput = True

# set required parameters 
in_features = "I:\\OSU\\Transfer\\data\\shp\\staticGTFS\\stops.shp"
near_features = ["I:\\OSU\\Transfer\\data\\shp\\staticGTFS\\stops.shp"]
out_table = "near_parks_trails"

# optional parameters
search_radius = '1000 Meters'
location = 'NO_LOCATION'
angle = 'NO_ANGLE'
closest = 'ALL'
closest_count = 12

# find crime locations within the search radius
arcpy.GenerateNearTable_analysis(in_features, near_features, out_table, search_radius, 
                                 location, angle, closest, closest_count)
