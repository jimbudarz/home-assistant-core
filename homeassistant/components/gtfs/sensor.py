"""Support for GTFS (Google/General Transport Format Schema)."""
from __future__ import annotations

import datetime
import logging
import os
import threading
from typing import Any

import pygtfs
from sqlalchemy.sql import text

from components.gtfs.const import ATTR_ARRIVAL, ATTR_BICYCLE, ATTR_DAY, ATTR_FIRST, ATTR_DROP_OFF_DESTINATION, \
    ATTR_DROP_OFF_ORIGIN, ATTR_INFO, ATTR_OFFSET, ATTR_LAST, ATTR_LOCATION_DESTINATION, ATTR_LOCATION_ORIGIN, \
    ATTR_PICKUP_DESTINATION, ATTR_PICKUP_ORIGIN, ATTR_ROUTE_TYPE, ATTR_TIMEPOINT_DESTINATION, ATTR_TIMEPOINT_ORIGIN, \
    ATTR_WHEELCHAIR, ATTR_WHEELCHAIR_DESTINATION, ATTR_WHEELCHAIR_ORIGIN, CONF_DATA, CONF_DESTINATION, CONF_ORIGIN, \
    CONF_TOMORROW, DEFAULT_NAME, DEFAULT_PATH, BICYCLE_ALLOWED_DEFAULT, BICYCLE_ALLOWED_OPTIONS, DROP_OFF_TYPE_DEFAULT, \
    DROP_OFF_TYPE_OPTIONS, ICON, ICONS, LOCATION_TYPE_DEFAULT, LOCATION_TYPE_OPTIONS, PICKUP_TYPE_DEFAULT, \
    PICKUP_TYPE_OPTIONS, ROUTE_TYPE_OPTIONS, TIMEPOINT_DEFAULT, TIMEPOINT_OPTIONS, WHEELCHAIR_ACCESS_DEFAULT, \
    WHEELCHAIR_ACCESS_OPTIONS, WHEELCHAIR_BOARDING_DEFAULT, WHEELCHAIR_BOARDING_OPTIONS
from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
)
from homeassistant.const import ATTR_ATTRIBUTION, CONF_NAME, CONF_OFFSET
from homeassistant.core import HomeAssistant
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType
from homeassistant.util import slugify
import homeassistant.util.dt as dt_util

_LOGGER = logging.getLogger(__name__)


def get_next_departure(
    schedule: Any,
    start_station_id: Any,
    end_station_id: Any,
    offset: cv.time_period,
    include_tomorrow: bool = False,
) -> dict:
    """Get the next departure for the given schedule."""
    now = dt_util.now().replace(tzinfo=None) + offset
    now_date = now.strftime(dt_util.DATE_STR_FORMAT)
    yesterday = now - datetime.timedelta(days=1)
    yesterday_date = yesterday.strftime(dt_util.DATE_STR_FORMAT)
    tomorrow = now + datetime.timedelta(days=1)
    tomorrow_date = tomorrow.strftime(dt_util.DATE_STR_FORMAT)

    # Fetch all departures for yesterday, today and optionally tomorrow,
    # up to an overkill maximum in case of a departure every minute for those
    # days.
    limit = 24 * 60 * 60 * 2
    tomorrow_select = tomorrow_where = tomorrow_order = ""
    if include_tomorrow:
        limit = int(limit / 2 * 3)
        tomorrow_name = tomorrow.strftime("%A").lower()
        tomorrow_select = f"calendar.{tomorrow_name} AS tomorrow,"
        tomorrow_where = f"OR calendar.{tomorrow_name} = 1"
        tomorrow_order = f"calendar.{tomorrow_name} DESC,"

    sql_query = f"""
        SELECT trip_candidate.trip_id, trip_candidate.route_id,
               time(origin_stop_time.arrival_time) AS origin_arrival_time,
               time(origin_stop_time.departure_time) AS origin_depart_time,
               date(origin_stop_time.departure_time) AS origin_depart_date,
               origin_stop_time.drop_off_type AS origin_drop_off_type,
               origin_stop_time.pickup_type AS origin_pickup_type,
               origin_stop_time.shape_dist_traveled AS origin_dist_traveled,
               origin_stop_time.stop_headsign AS origin_stop_headsign,
               origin_stop_time.stop_sequence AS origin_stop_sequence,
               origin_stop_time.timepoint AS origin_stop_timepoint,
               time(destination_stop_time.arrival_time) AS dest_arrival_time,
               time(destination_stop_time.departure_time) AS dest_depart_time,
               destination_stop_time.drop_off_type AS dest_drop_off_type,
               destination_stop_time.pickup_type AS dest_pickup_type,
               destination_stop_time.shape_dist_traveled AS dest_dist_traveled,
               destination_stop_time.stop_headsign AS dest_stop_headsign,
               destination_stop_time.stop_sequence AS dest_stop_sequence,
               destination_stop_time.timepoint AS dest_stop_timepoint,
               calendar.{yesterday.strftime("%A").lower()} AS yesterday,
               calendar.{now.strftime("%A").lower()} AS today,
               {tomorrow_select}
               calendar.start_date AS start_date,
               calendar.end_date AS end_date
        FROM trips trip_candidate
        INNER JOIN calendar calendar
                   ON trip_candidate.service_id = calendar.service_id
        INNER JOIN stop_times origin_stop_time
                   ON trip_candidate.trip_id = origin_stop_time.trip_id
        INNER JOIN stops start_station
                   ON origin_stop_time.stop_id = start_station.stop_id
        INNER JOIN stop_times destination_stop_time
                   ON trip_candidate.trip_id = destination_stop_time.trip_id
        INNER JOIN stops end_station
                   ON destination_stop_time.stop_id = end_station.stop_id
        WHERE (calendar.{yesterday.strftime("%A").lower()} = 1
               OR calendar.{now.strftime("%A").lower()} = 1
               {tomorrow_where}
               )
        AND start_station.stop_id = :origin_station_id
                   AND end_station.stop_id = :end_station_id
        AND origin_stop_sequence < dest_stop_sequence
        AND calendar.start_date <= :today
        AND calendar.end_date >= :today
        ORDER BY calendar.{yesterday.strftime("%A").lower()} DESC,
                 calendar.{now.strftime("%A").lower()} DESC,
                 {tomorrow_order}
                 origin_stop_time.departure_time
        LIMIT :limit
        """
    departures = schedule.engine.execute(
        text(sql_query),
        origin_station_id=start_station_id,
        end_station_id=end_station_id,
        today=now_date,
        limit=limit,
    )

    timetable = create_lookup_timetable(departures, now_date, tomorrow_date, yesterday_date)

    _LOGGER.debug("Timetable: %s", sorted(timetable.keys()))

    trip_candidate = {}
    for key in sorted(timetable.keys()):
        if dt_util.parse_datetime(key) > now:
            trip_candidate = timetable[key]
            _LOGGER.debug(
                "Departure found for station %s @ %s -> %s", start_station_id, key, trip_candidate
            )
            break

    if trip_candidate == {}:
        return {}

    # Format arrival and departure dates and times, accounting for the
    # possibility of times crossing over midnight.
    origin_arrival = now
    if trip_candidate["origin_arrival_time"] > trip_candidate["origin_depart_time"]:
        origin_arrival -= datetime.timedelta(days=1)
    origin_arrival_time = (
        f"{origin_arrival.strftime(dt_util.DATE_STR_FORMAT)} "
        f"{trip_candidate['origin_arrival_time']}"
    )

    origin_depart_time = f"{now_date} {trip_candidate['origin_depart_time']}"

    dest_arrival = now
    if trip_candidate["dest_arrival_time"] < trip_candidate["origin_depart_time"]:
        dest_arrival += datetime.timedelta(days=1)
    dest_arrival_time = (
        f"{dest_arrival.strftime(dt_util.DATE_STR_FORMAT)} "
        f"{trip_candidate['dest_arrival_time']}"
    )

    dest_depart = dest_arrival
    if trip_candidate["dest_depart_time"] < trip_candidate["dest_arrival_time"]:
        dest_depart += datetime.timedelta(days=1)
    dest_depart_time = (
        f"{dest_depart.strftime(dt_util.DATE_STR_FORMAT)} "
        f"{trip_candidate['dest_depart_time']}"
    )

    depart_time = dt_util.parse_datetime(origin_depart_time)
    arrival_time = dt_util.parse_datetime(dest_arrival_time)

    origin_stop_time = {
        "Arrival Time": origin_arrival_time,
        "Departure Time": origin_depart_time,
        "Drop Off Type": trip_candidate["origin_drop_off_type"],
        "Pickup Type": trip_candidate["origin_pickup_type"],
        "Shape Dist Traveled": trip_candidate["origin_dist_traveled"],
        "Headsign": trip_candidate["origin_stop_headsign"],
        "Sequence": trip_candidate["origin_stop_sequence"],
        "Timepoint": trip_candidate["origin_stop_timepoint"],
    }

    destination_stop_time = {
        "Arrival Time": dest_arrival_time,
        "Departure Time": dest_depart_time,
        "Drop Off Type": trip_candidate["dest_drop_off_type"],
        "Pickup Type": trip_candidate["dest_pickup_type"],
        "Shape Dist Traveled": trip_candidate["dest_dist_traveled"],
        "Headsign": trip_candidate["dest_stop_headsign"],
        "Sequence": trip_candidate["dest_stop_sequence"],
        "Timepoint": trip_candidate["dest_stop_timepoint"],
    }

    return {
        "trip_id": trip_candidate["trip_id"],
        "route_id": trip_candidate["route_id"],
        "day": trip_candidate["day"],
        "first": trip_candidate["first"],
        "last": trip_candidate["last"],
        "departure_time": depart_time,
        "arrival_time": arrival_time,
        "origin_stop_time": origin_stop_time,
        "destination_stop_time": destination_stop_time,
    }


def create_lookup_timetable(
        departures: Any,
        now_date: str,
        tomorrow_date: str,
        yesterday_date: str,
):
    # Create lookup timetable for today and possibly tomorrow, taking into
    # account any departures from yesterday scheduled after midnight,
    # as long as all departures are within the calendar date range.
    timetable = {}
    yesterday_start = today_start = tomorrow_start = None
    yesterday_last = today_last = ""
    for departure in departures:
        if departure["yesterday"] == 1 and yesterday_date >= departure["start_date"]:
            extras = {"day": "yesterday", "first": None, "last": False}
            if yesterday_start is None:
                yesterday_start = departure["origin_depart_date"]
            if yesterday_start != departure["origin_depart_date"]:
                idx = f"{now_date} {departure['origin_depart_time']}"
                timetable[idx] = {**departure, **extras}
                yesterday_last = idx

        if departure["today"] == 1:
            extras = {"day": "today", "first": False, "last": False}
            if today_start is None:
                today_start = departure["origin_depart_date"]
                extras["first"] = True
            if today_start == departure["origin_depart_date"]:
                idx_prefix = now_date
            else:
                idx_prefix = tomorrow_date
            idx = f"{idx_prefix} {departure['origin_depart_time']}"
            timetable[idx] = {**departure, **extras}
            today_last = idx

        if (
                "tomorrow" in departure
                and departure["tomorrow"] == 1
                and tomorrow_date <= departure["end_date"]
        ):
            extras = {"day": "tomorrow", "first": False, "last": None}
            if tomorrow_start is None:
                tomorrow_start = departure["origin_depart_date"]
                extras["first"] = True
            if tomorrow_start == departure["origin_depart_date"]:
                idx = f"{tomorrow_date} {departure['origin_depart_time']}"
                timetable[idx] = {**departure, **extras}

    # Flag last departures.
    for idx in filter(None, [yesterday_last, today_last]):
        timetable[idx]["last"] = True

    return timetable


def setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None,
) -> None:
    """Set up the GTFS sensor."""
    gtfs_dir = hass.config.path(DEFAULT_PATH)
    data = config[CONF_DATA]
    origin = config.get(CONF_ORIGIN)
    destination = config.get(CONF_DESTINATION)
    name = config.get(CONF_NAME)
    offset: datetime.timedelta = config[CONF_OFFSET]
    include_tomorrow = config[CONF_TOMORROW]

    os.makedirs(gtfs_dir, exist_ok=True)

    if not os.path.exists(os.path.join(gtfs_dir, data)):
        _LOGGER.error("The given GTFS data file/folder was not found")
        return

    (gtfs_root, _) = os.path.splitext(data)

    sqlite_file = f"{gtfs_root}.sqlite?check_same_thread=False"
    joined_path = os.path.join(gtfs_dir, sqlite_file)
    gtfs = pygtfs.Schedule(joined_path)

    # pylint: disable=no-member
    if not gtfs.feeds:
        pygtfs.append_feed(gtfs, os.path.join(gtfs_dir, data))

    add_entities(
        [GTFSDepartureSensor(gtfs, name, origin, destination, offset, include_tomorrow)]
    )


class GTFSDepartureSensor(SensorEntity):
    """Implementation of a GTFS departure sensor."""

    _attr_device_class = SensorDeviceClass.TIMESTAMP

    def __init__(
        self,
        gtfs: Any,
        name: Any | None,
        origin: Any,
        destination: Any,
        offset: datetime.timedelta,
        include_tomorrow: bool,
    ) -> None:
        """Initialize the sensor."""
        self._pygtfs = gtfs
        self.origin = origin
        self.destination = destination
        self._include_tomorrow = include_tomorrow
        self._offset = offset
        self._custom_name = name

        self._available = False
        self._icon = ICON
        self._name = ""
        self._state: datetime.datetime | None = None
        self._attributes: dict[str, Any] = {}

        self._agency = None
        self._departure: dict[str, Any] = {}
        self._destination = None
        self._origin = None
        self._route = None
        self._trip = None

        self.lock = threading.Lock()
        self.update()

    @property
    def name(self) -> str:
        """Return the name of the sensor."""
        return self._name

    @property
    def native_value(self) -> datetime.datetime | None:
        """Return the state of the sensor."""
        return self._state

    @property
    def available(self) -> bool:
        """Return True if entity is available."""
        return self._available

    @property
    def extra_state_attributes(self) -> dict:
        """Return the state attributes."""
        return self._attributes

    @property
    def icon(self) -> str:
        """Icon to use in the frontend, if any."""
        return self._icon

    def update(self) -> None:
        """Get the latest data from GTFS and update the states."""
        with self.lock:
            # Fetch valid stop information once
            if not self._origin:
                stops = self._pygtfs.stops_by_id(self.origin)
                if not stops:
                    self._available = False
                    _LOGGER.warning("Origin stop ID %s not found", self.origin)
                    return
                self._origin = stops[0]

            if not self._destination:
                stops = self._pygtfs.stops_by_id(self.destination)
                if not stops:
                    self._available = False
                    _LOGGER.warning(
                        "Destination stop ID %s not found", self.destination
                    )
                    return
                self._destination = stops[0]

            self._available = True

            # Fetch next departure
            self._departure = get_next_departure(
                self._pygtfs,
                self.origin,
                self.destination,
                self._offset,
                self._include_tomorrow,
            )

            # Define the state as a UTC timestamp with ISO 8601 format
            if not self._departure:
                self._state = None
            else:
                self._state = self._departure["departure_time"].replace(
                    tzinfo=dt_util.UTC
                )

            # Fetch trip and route details once, unless updated
            if not self._departure:
                self._trip = None
            else:
                trip_id = self._departure["trip_id"]
                if not self._trip or self._trip.trip_id != trip_id:
                    _LOGGER.debug("Fetching trip details for %s", trip_id)
                    self._trip = self._pygtfs.trips_by_id(trip_id)[0]

                route_id = self._departure["route_id"]
                if not self._route or self._route.route_id != route_id:
                    _LOGGER.debug("Fetching route details for %s", route_id)
                    self._route = self._pygtfs.routes_by_id(route_id)[0]

            # Fetch agency details exactly once
            if self._agency is None and self._route:
                _LOGGER.debug("Fetching agency details for %s", self._route.agency_id)
                try:
                    self._agency = self._pygtfs.agencies_by_id(self._route.agency_id)[0]
                except IndexError:
                    _LOGGER.warning(
                        "Agency ID '%s' was not found in agency table, "
                        "you may want to update the routes database table "
                        "to fix this missing reference",
                        self._route.agency_id,
                    )
                    self._agency = False

            # Assign attributes, icon and name
            self.update_attributes()

            if self._route:
                self._icon = ICONS.get(self._route.route_type, ICON)
            else:
                self._icon = ICON

            name = (
                f"{getattr(self._agency, 'agency_name', DEFAULT_NAME)} "
                f"{self.origin} to {self.destination} next departure"
            )
            if not self._departure:
                name = f"{DEFAULT_NAME}"
            self._name = self._custom_name or name

    def update_attributes(self) -> None:
        """Update state attributes."""
        # Add departure information
        if self._departure:
            self._attributes[ATTR_ARRIVAL] = dt_util.as_utc(
                self._departure["arrival_time"]
            ).isoformat()

            self._attributes[ATTR_DAY] = self._departure["day"]

            if self._departure[ATTR_FIRST] is not None:
                self._attributes[ATTR_FIRST] = self._departure["first"]
            elif ATTR_FIRST in self._attributes:
                del self._attributes[ATTR_FIRST]

            if self._departure[ATTR_LAST] is not None:
                self._attributes[ATTR_LAST] = self._departure["last"]
            elif ATTR_LAST in self._attributes:
                del self._attributes[ATTR_LAST]
        else:
            if ATTR_ARRIVAL in self._attributes:
                del self._attributes[ATTR_ARRIVAL]
            if ATTR_DAY in self._attributes:
                del self._attributes[ATTR_DAY]
            if ATTR_FIRST in self._attributes:
                del self._attributes[ATTR_FIRST]
            if ATTR_LAST in self._attributes:
                del self._attributes[ATTR_LAST]

        # Add contextual information
        self._attributes[ATTR_OFFSET] = self._offset.total_seconds() / 60

        if self._state is None:
            self._attributes[ATTR_INFO] = (
                "No more departures"
                if self._include_tomorrow
                else "No more departures today"
            )
        elif ATTR_INFO in self._attributes:
            del self._attributes[ATTR_INFO]

        if self._agency:
            self._attributes[ATTR_ATTRIBUTION] = self._agency.agency_name
        elif ATTR_ATTRIBUTION in self._attributes:
            del self._attributes[ATTR_ATTRIBUTION]

        # Add extra metadata
        key = "agency_id"
        if self._agency and key not in self._attributes:
            self.append_keys(self.dict_for_table(self._agency), "Agency")

        key = "origin_station_stop_id"
        if self._origin and key not in self._attributes:
            self.append_keys(self.dict_for_table(self._origin), "Origin Station")
            self._attributes[ATTR_LOCATION_ORIGIN] = LOCATION_TYPE_OPTIONS.get(
                self._origin.location_type, LOCATION_TYPE_DEFAULT
            )
            self._attributes[ATTR_WHEELCHAIR_ORIGIN] = WHEELCHAIR_BOARDING_OPTIONS.get(
                self._origin.wheelchair_boarding, WHEELCHAIR_BOARDING_DEFAULT
            )

        key = "destination_station_stop_id"
        if self._destination and key not in self._attributes:
            self.append_keys(
                self.dict_for_table(self._destination), "Destination Station"
            )
            self._attributes[ATTR_LOCATION_DESTINATION] = LOCATION_TYPE_OPTIONS.get(
                self._destination.location_type, LOCATION_TYPE_DEFAULT
            )
            self._attributes[
                ATTR_WHEELCHAIR_DESTINATION
            ] = WHEELCHAIR_BOARDING_OPTIONS.get(
                self._destination.wheelchair_boarding, WHEELCHAIR_BOARDING_DEFAULT
            )

        # Manage Route metadata
        key = "route_id"
        if not self._route and key in self._attributes:
            self.remove_keys("Route")
        elif self._route and (
            key not in self._attributes or self._attributes[key] != self._route.route_id
        ):
            self.append_keys(self.dict_for_table(self._route), "Route")
            self._attributes[ATTR_ROUTE_TYPE] = ROUTE_TYPE_OPTIONS[
                self._route.route_type
            ]

        # Manage Trip metadata
        key = "trip_id"
        if not self._trip and key in self._attributes:
            self.remove_keys("Trip")
        elif self._trip and (
            key not in self._attributes or self._attributes[key] != self._trip.trip_id
        ):
            self.append_keys(self.dict_for_table(self._trip), "Trip")
            self._attributes[ATTR_BICYCLE] = BICYCLE_ALLOWED_OPTIONS.get(
                self._trip.bikes_allowed, BICYCLE_ALLOWED_DEFAULT
            )
            self._attributes[ATTR_WHEELCHAIR] = WHEELCHAIR_ACCESS_OPTIONS.get(
                self._trip.wheelchair_accessible, WHEELCHAIR_ACCESS_DEFAULT
            )

        # Manage Stop Times metadata
        prefix = "origin_stop"
        if self._departure:
            self.append_keys(self._departure["origin_stop_time"], prefix)
            self._attributes[ATTR_DROP_OFF_ORIGIN] = DROP_OFF_TYPE_OPTIONS.get(
                self._departure["origin_stop_time"]["Drop Off Type"],
                DROP_OFF_TYPE_DEFAULT,
            )
            self._attributes[ATTR_PICKUP_ORIGIN] = PICKUP_TYPE_OPTIONS.get(
                self._departure["origin_stop_time"]["Pickup Type"], PICKUP_TYPE_DEFAULT
            )
            self._attributes[ATTR_TIMEPOINT_ORIGIN] = TIMEPOINT_OPTIONS.get(
                self._departure["origin_stop_time"]["Timepoint"], TIMEPOINT_DEFAULT
            )
        else:
            self.remove_keys(prefix)

        prefix = "destination_stop"
        if self._departure:
            self.append_keys(self._departure["destination_stop_time"], prefix)
            self._attributes[ATTR_DROP_OFF_DESTINATION] = DROP_OFF_TYPE_OPTIONS.get(
                self._departure["destination_stop_time"]["Drop Off Type"],
                DROP_OFF_TYPE_DEFAULT,
            )
            self._attributes[ATTR_PICKUP_DESTINATION] = PICKUP_TYPE_OPTIONS.get(
                self._departure["destination_stop_time"]["Pickup Type"],
                PICKUP_TYPE_DEFAULT,
            )
            self._attributes[ATTR_TIMEPOINT_DESTINATION] = TIMEPOINT_OPTIONS.get(
                self._departure["destination_stop_time"]["Timepoint"], TIMEPOINT_DEFAULT
            )
        else:
            self.remove_keys(prefix)

    @staticmethod
    def dict_for_table(resource: Any) -> dict:
        """Return a dictionary for the SQLAlchemy resource given."""
        return {
            col: getattr(resource, col) for col in resource.__table__.columns.keys()
        }

    def append_keys(self, resource: dict, prefix: str | None = None) -> None:
        """Properly format key val pairs to append to attributes."""
        for attr, val in resource.items():
            if val == "" or val is None or attr == "feed_id":
                continue
            key = attr
            if prefix and not key.startswith(prefix):
                key = f"{prefix} {key}"
            key = slugify(key)
            self._attributes[key] = val

    def remove_keys(self, prefix: str) -> None:
        """Remove attributes whose key starts with prefix."""
        self._attributes = {
            k: v for k, v in self._attributes.items() if not k.startswith(prefix)
        }
