#!/usr/bin/env python3
"""
Script to fetch GTFS-RT vehicle positions and dump all available fields
"""

import asyncio
import httpx
from google.transit import gtfs_realtime_pb2
from datetime import datetime


GTFS_API_URL = "https://gateway.carris.pt/gateway/gtfs/api/v2.11/GTFS/realtime/vehiclepositions"


async def fetch_and_dump():
    """Fetch vehicle positions and display all fields"""
    
    print(f"Fetching vehicle positions from: {GTFS_API_URL}")
    print("=" * 80)
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(GTFS_API_URL)
            response.raise_for_status()
            
            # Parse protobuf
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(response.content)
            
            print(f"\n📊 Feed Header:")
            print(f"  - GTFS Realtime Version: {feed.header.gtfs_realtime_version}")
            print(f"  - Incrementality: {feed.header.incrementality}")
            print(f"  - Timestamp: {feed.header.timestamp} ({datetime.fromtimestamp(feed.header.timestamp)})")
            
            print(f"\n🚍 Total Vehicles: {len(feed.entity)}")
            print("=" * 80)
            
            # Display first 3 vehicles with all fields
            for i, entity in enumerate(feed.entity[:3], 1):
                if not entity.HasField('vehicle'):
                    continue
                
                vehicle = entity.vehicle
                
                print(f"\n{'='*80}")
                print(f"Vehicle #{i} - Entity ID: {entity.id}")
                print(f"{'='*80}")
                
                # Vehicle descriptor
                print("\n🚌 VEHICLE DESCRIPTOR:")
                if vehicle.HasField('vehicle'):
                    print(f"  ✓ id: {vehicle.vehicle.id if vehicle.vehicle.HasField('id') else 'N/A'}")
                    print(f"  ✓ label: {vehicle.vehicle.label if vehicle.vehicle.HasField('label') else 'N/A'}")
                    print(f"  ✓ license_plate: {vehicle.vehicle.license_plate if vehicle.vehicle.HasField('license_plate') else 'N/A'}")
                else:
                    print("  ✗ No vehicle descriptor")
                
                # Trip descriptor
                print("\n🗺️  TRIP DESCRIPTOR:")
                if vehicle.HasField('trip'):
                    print(f"  ✓ trip_id: {vehicle.trip.trip_id if vehicle.trip.HasField('trip_id') else 'N/A'}")
                    print(f"  ✓ route_id: {vehicle.trip.route_id if vehicle.trip.HasField('route_id') else 'N/A'}")
                    print(f"  ✓ direction_id: {vehicle.trip.direction_id if vehicle.trip.HasField('direction_id') else 'N/A'}")
                    print(f"  ✓ start_time: {vehicle.trip.start_time if vehicle.trip.HasField('start_time') else 'N/A'}")
                    print(f"  ✓ start_date: {vehicle.trip.start_date if vehicle.trip.HasField('start_date') else 'N/A'}")
                    print(f"  ✓ schedule_relationship: {vehicle.trip.schedule_relationship if vehicle.trip.HasField('schedule_relationship') else 'N/A'}")
                else:
                    print("  ✗ No trip descriptor")
                
                # Position
                print("\n📍 POSITION:")
                if vehicle.HasField('position'):
                    print(f"  ✓ latitude: {vehicle.position.latitude}")
                    print(f"  ✓ longitude: {vehicle.position.longitude}")
                    print(f"  ✓ bearing: {vehicle.position.bearing if vehicle.position.HasField('bearing') else 'N/A'}")
                    print(f"  ✓ odometer: {vehicle.position.odometer if vehicle.position.HasField('odometer') else 'N/A'}")
                    print(f"  ✓ speed: {vehicle.position.speed if vehicle.position.HasField('speed') else 'N/A'}")
                else:
                    print("  ✗ No position")
                
                # Status fields
                print("\n📊 STATUS:")
                print(f"  ✓ current_stop_sequence: {vehicle.current_stop_sequence if vehicle.HasField('current_stop_sequence') else 'N/A'}")
                print(f"  ✓ stop_id: {vehicle.stop_id if vehicle.HasField('stop_id') else 'N/A'}")
                
                if vehicle.HasField('current_status'):
                    status_map = {
                        0: "INCOMING_AT",
                        1: "STOPPED_AT",
                        2: "IN_TRANSIT_TO",
                    }
                    print(f"  ✓ current_status: {status_map.get(vehicle.current_status, 'UNKNOWN')} ({vehicle.current_status})")
                else:
                    print(f"  ✓ current_status: N/A")
                
                # Timestamp
                print("\n🕐 TIMESTAMP:")
                if vehicle.HasField('timestamp'):
                    dt = datetime.fromtimestamp(vehicle.timestamp)
                    age = datetime.now().timestamp() - vehicle.timestamp
                    print(f"  ✓ timestamp: {vehicle.timestamp}")
                    print(f"  ✓ datetime: {dt}")
                    print(f"  ✓ age: {age:.1f} seconds")
                else:
                    print("  ✗ No timestamp")
                
                # Congestion level
                print("\n🚦 CONGESTION:")
                if vehicle.HasField('congestion_level'):
                    congestion_map = {
                        0: "UNKNOWN_CONGESTION_LEVEL",
                        1: "RUNNING_SMOOTHLY",
                        2: "STOP_AND_GO",
                        3: "CONGESTION",
                        4: "SEVERE_CONGESTION",
                    }
                    print(f"  ✓ congestion_level: {congestion_map.get(vehicle.congestion_level, 'UNKNOWN')} ({vehicle.congestion_level})")
                else:
                    print("  ✗ No congestion level")
                
                # Occupancy status
                print("\n👥 OCCUPANCY:")
                if vehicle.HasField('occupancy_status'):
                    occupancy_map = {
                        0: "EMPTY",
                        1: "MANY_SEATS_AVAILABLE",
                        2: "FEW_SEATS_AVAILABLE",
                        3: "STANDING_ROOM_ONLY",
                        4: "CRUSHED_STANDING_ROOM_ONLY",
                        5: "FULL",
                        6: "NOT_ACCEPTING_PASSENGERS",
                    }
                    print(f"  ✓ occupancy_status: {occupancy_map.get(vehicle.occupancy_status, 'UNKNOWN')} ({vehicle.occupancy_status})")
                else:
                    print("  ✗ No occupancy status")
            
            print(f"\n{'='*80}")
            print(f"Displayed {min(3, len(feed.entity))} of {len(feed.entity)} vehicles")
            print(f"{'='*80}\n")
            
        except httpx.HTTPStatusError as e:
            print(f"❌ HTTP Error: {e.response.status_code}")
        except httpx.RequestError as e:
            print(f"❌ Request Error: {e}")
        except Exception as e:
            print(f"❌ Unexpected Error: {e}")


if __name__ == "__main__":
    asyncio.run(fetch_and_dump())
