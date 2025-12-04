"""
Test script to visualize vehicle tracking with shape matching and speed calculation.
Plots:
- GTFS shape polyline (solid line)
- Real vehicle GPS points (markers with dotted line)
- Shape-matched positions
- Speed calculations
"""
import asyncio
import redis.asyncio as redis
from src.config import settings
from src.gtfs_enrichment import GTFSEnrichment
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime


async def get_vehicle_track(vehicle_id: str):
    """Get the recent tracking data for a vehicle from Redis."""
    redis_client = redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        password=settings.redis_password,
        decode_responses=True
    )
    
    try:
        # Get current vehicle state
        vehicle_data = await redis_client.hgetall(f"vehicle:{vehicle_id}")
        
        if not vehicle_data:
            print(f"No data found for vehicle {vehicle_id}")
            return None
        
        # Get trip track (last 50 positions)
        trip_id = vehicle_data.get('trip_id')
        service_date = vehicle_data.get('service_date', '')
        if trip_id and service_date:
            track = await redis_client.xrevrange(
                f"trip:{trip_id}:{service_date}:track",
                count=50
            )
            return {
                'vehicle_id': vehicle_id,
                'trip_id': trip_id,
                'route_id': vehicle_data.get('route_id'),
                'service_date': service_date,
                'current_state': vehicle_data,
                'track': track
            }
        elif trip_id and not service_date:
            print(f"Warning: Vehicle {vehicle_id} has trip_id but no service_date")
        return None
    finally:
        await redis_client.aclose()


async def plot_vehicle_tracking(vehicle_id: str):
    """Plot vehicle tracking with shape matching."""
    # Load GTFS data
    print("Loading GTFS data...")
    gtfs = GTFSEnrichment(settings.db_url)
    await gtfs.connect()
    await gtfs.load_data_on_startup()
    
    # Get vehicle tracking data
    print(f"Getting data for vehicle {vehicle_id}...")
    data = await get_vehicle_track(vehicle_id)
    
    if not data:
        return
    
    trip_id = data['trip_id']
    route_id = data['route_id']
    
    print(f"Trip: {trip_id}")
    print(f"Route: {route_id}")
    
    # Get shape for this trip
    trip = gtfs.trips.get(trip_id)
    if not trip or 'shape_id' not in trip:
        print(f"No shape found for trip {trip_id}")
        return
    
    shape_id = trip['shape_id']
    shape_points = gtfs.shapes.get(shape_id, [])
    
    if not shape_points:
        print(f"No shape points found for shape {shape_id}")
        return
    
    print(f"Shape: {shape_id} ({len(shape_points)} points)")
    
    # Extract GPS points from track
    gps_points = []
    timestamps = []
    shape_speeds = []
    
    for entry_id, entry_data in reversed(data['track']):  # Reverse to get chronological order
        lat = float(entry_data.get('latitude', 0))
        lon = float(entry_data.get('longitude', 0))
        ts = int(entry_data.get('timestamp', 0))
        speed = entry_data.get('shape_speed', '')
        
        if lat and lon:
            gps_points.append((lat, lon))
            timestamps.append(ts)
            shape_speeds.append(float(speed) if speed else None)
    
    print(f"GPS points: {len(gps_points)}")
    print(f"Speed calculations: {sum(1 for s in shape_speeds if s is not None)}")
    
    # Create plot
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # Plot 1: Map view
    # Plot shape
    shape_lats = [p['lat'] for p in shape_points]
    shape_lons = [p['lon'] for p in shape_points]
    ax1.plot(shape_lons, shape_lats, 'b-', linewidth=2, label='GTFS Shape', alpha=0.6)
    
    # Plot GPS points with dotted line
    if gps_points:
        gps_lats, gps_lons = zip(*gps_points)
        ax1.plot(gps_lons, gps_lats, 'r:', linewidth=1, alpha=0.5, label='GPS Track')
        ax1.scatter(gps_lons, gps_lats, c='red', s=50, zorder=5, label='GPS Points', alpha=0.7)
        
        # Add arrows to show direction
        for i in range(0, len(gps_points)-1, max(1, len(gps_points)//10)):
            dx = gps_lons[i+1] - gps_lons[i]
            dy = gps_lats[i+1] - gps_lats[i]
            ax1.arrow(gps_lons[i], gps_lats[i], dx*0.3, dy*0.3, 
                     head_width=0.0001, head_length=0.0001, fc='red', ec='red', alpha=0.5)
    
    ax1.set_xlabel('Longitude')
    ax1.set_ylabel('Latitude')
    ax1.set_title(f'Vehicle {vehicle_id} - Route {route_id}\nTrip {trip_id}')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.axis('equal')
    
    # Plot 2: Speed over time
    if shape_speeds and any(s is not None for s in shape_speeds):
        valid_speeds = [(i, s*3.6) for i, s in enumerate(shape_speeds) if s is not None]  # Convert to km/h
        if valid_speeds:
            indices, speeds_kmh = zip(*valid_speeds)
            
            # Calculate time deltas from first point
            time_deltas = [(timestamps[i] - timestamps[0]) for i in indices]
            
            ax2.plot(time_deltas, speeds_kmh, 'g-o', linewidth=2, markersize=6)
            ax2.set_xlabel('Time (seconds from start)')
            ax2.set_ylabel('Speed (km/h)')
            ax2.set_title('Shape-based Speed Calculation')
            ax2.grid(True, alpha=0.3)
            ax2.axhline(y=0, color='k', linestyle='--', alpha=0.3)
            
            # Add statistics
            avg_speed = np.mean(speeds_kmh)
            max_speed = np.max(speeds_kmh)
            ax2.text(0.02, 0.98, f'Avg: {avg_speed:.1f} km/h\nMax: {max_speed:.1f} km/h',
                    transform=ax2.transAxes, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    else:
        ax2.text(0.5, 0.5, 'No speed data available', 
                ha='center', va='center', transform=ax2.transAxes, fontsize=14)
        ax2.set_title('Shape-based Speed Calculation')
    
    plt.tight_layout()
    
    # Save plot
    filename = f'vehicle_{vehicle_id}_tracking.png'
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    print(f"\nPlot saved to: {filename}")
    
    plt.show()
    
    # Print detailed statistics
    print("\n" + "="*60)
    print("TRACKING STATISTICS")
    print("="*60)
    print(f"Vehicle ID: {vehicle_id}")
    print(f"Route: {route_id}")
    print(f"Trip: {trip_id}")
    print(f"Shape: {shape_id}")
    print(f"GPS Points: {len(gps_points)}")
    print(f"Speed Calculations: {sum(1 for s in shape_speeds if s is not None)}")
    
    if shape_speeds and any(s is not None for s in shape_speeds):
        valid_speeds_kmh = [s*3.6 for s in shape_speeds if s is not None]
        print(f"\nSpeed Statistics (km/h):")
        print(f"  Min: {min(valid_speeds_kmh):.1f}")
        print(f"  Max: {max(valid_speeds_kmh):.1f}")
        print(f"  Avg: {np.mean(valid_speeds_kmh):.1f}")
        print(f"  Median: {np.median(valid_speeds_kmh):.1f}")
    
    # Print last few track points with details
    print(f"\nLast 5 Tracking Points:")
    print("-" * 60)
    for i, (entry_id, entry_data) in enumerate(list(data['track'])[:5]):
        ts = int(entry_data.get('timestamp', 0))
        dt = datetime.fromtimestamp(ts)
        lat = entry_data.get('latitude', 'N/A')
        lon = entry_data.get('longitude', 'N/A')
        speed = entry_data.get('shape_speed', 'N/A')
        dist = entry_data.get('shape_dist_traveled', 'N/A')
        
        if speed != 'N/A' and speed:
            speed_kmh = f"{float(speed)*3.6:.1f} km/h"
        else:
            speed_kmh = 'N/A'
        
        print(f"{i+1}. {dt.strftime('%H:%M:%S')}")
        print(f"   GPS: ({lat}, {lon})")
        print(f"   Speed: {speed_kmh}")
        print(f"   Distance: {dist}m")
        print()


async def list_active_vehicles():
    """List currently active vehicles."""
    redis_client = redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        password=settings.redis_password,
        decode_responses=True
    )
    
    try:
        # Get all active vehicles
        vehicle_ids = await redis_client.smembers('active_vehicles')
        
        if not vehicle_ids:
            print("No active vehicles found")
            return []
        
        print(f"Found {len(vehicle_ids)} active vehicles")
        print("\nVehicle ID | Route | Trip ID | Status")
        print("-" * 60)
        
        active_with_trips = []
        for vid in sorted(vehicle_ids):
            data = await redis_client.hgetall(f"vehicle:{vid}")
            if data and data.get('trip_id'):
                status = data.get('status', 'unknown')
                route = data.get('route_id', 'N/A')
                trip = data.get('trip_id', 'N/A')
                print(f"{vid:10} | {route:5} | {trip:30} | {status}")
                
                if status == 'active':
                    active_with_trips.append(vid)
        
        return active_with_trips
    finally:
        await redis_client.aclose()


async def main():
    """Main test function."""
    print("="*60)
    print("VEHICLE SHAPE SPEED VISUALIZATION TEST")
    print("="*60)
    print()
    
    # List active vehicles
    active_vehicles = await list_active_vehicles()
    
    if not active_vehicles:
        print("\nNo active vehicles with trips. Please wait for vehicles to become active.")
        return
    
    print(f"\n{len(active_vehicles)} active vehicles available for testing")
    
    # Use first active vehicle or prompt for input
    vehicle_id = input(f"\nEnter vehicle ID to visualize (or press Enter for {active_vehicles[0]}): ").strip()
    
    if not vehicle_id:
        vehicle_id = active_vehicles[0]
    
    print()
    await plot_vehicle_tracking(vehicle_id)


if __name__ == "__main__":
    asyncio.run(main())
