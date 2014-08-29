SELECT
  nation.name,
  sum(lineitem.extendedprice * (1 - lineitem.discount)) AS revenue 
FROM
  nation, customer, orders, lineitem
WHERE
  customer.custkey = orders.custkey
  and lineitem.orderkey = orders.orderkey
  and customer.nationkey = nation.nationkey 
  and orders.orderdate >= DATE('1994-01-01')
  and orders.orderdate < DATE ('1994-02-01')
GROUP BY nation.name
ORDER BY revenue desc;

