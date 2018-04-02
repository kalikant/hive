create table tree(id int,parent int);
insert into tree values(1,null);
insert into tree values(2,1);
insert into tree values(4,2);
insert into tree values(3,1);
insert into tree values(5,3);


==============================================================
this soution save on memory
=============================================================
select ax.parent as id, 
case when ax.parent is null then 'ROOT'
  when cast(ax.degree as int)=0 then 'LEAFE' 
   when cast (ax.degree as int)>0 then 'INNER'
end output
 from 
(
select parent,count(id) as degree from tree group by parent
union
select id,0 from tree a
left outer join
(select parent,count(id) as degree from tree group by parent) b
on a.id=b.parent
where b.parent is null
)ax

============================================================
this will consume more memory but simple to implement
=============================================================

select distinct a.id, 
case when a.parent is null then 'root'
   when b.id is null then 'leafe'
   else 'inner'
end
 from tree a
left outer join
tree b
on a.id = b.parent


