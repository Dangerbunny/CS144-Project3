create table SpatialLocation(
	ItemId bigint,
	locPt point,
	spatial index(locPt)) engine=MyISAM;


insert into SpatialLocation (ItemId, locPt)
select il.ItemId, Point(l.lat,l.lon)
from ItemLocation il, Location l
where il.LocId = l.LocId
and l.lat is not null
and l.lon is not null;