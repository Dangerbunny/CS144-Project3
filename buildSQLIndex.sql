create table SpatialLocation(
	ItemId bigint NOT NULL,
	locPt point NOT NULL,
	spatial index(locPt)) engine=MyISAM;


insert into SpatialLocation (ItemId, locPt)
    (select il.ItemId, Point(l.lat,l.lon)
    from ItemLocation il, Location l
    where il.LocId = l.LocId
    and l.lat is not null
    and l.lon is not null);