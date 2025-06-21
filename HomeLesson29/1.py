"""Создание постов:
CREATE 
  (:Post {id: 1, title: "Как начать учить Neo4j", content: "Это мой первый пост о графовой БД", created_at: datetime()}),
  (:Post {id: 2, title: "Работа с агрегациями в SQL", content: "SQL — основа аналитики", created_at: datetime()}),
  (:Post {id: 3, title: "Зачем нужна денормализация", content: "Нормализация vs производительность", created_at: datetime()});

#######################################################################################
Создание лайков:
CREATE 
  (:Like {like_id: 101, timestamp: datetime()}),
  (:Like {like_id: 102, timestamp: datetime()}),
  (:Like {like_id: 103, timestamp: datetime()}),
  (:Like {like_id: 104, timestamp: datetime()})

######################################################################################
Связи к новым объектам (7 штук)
Пользователи пишут посты:

MATCH (a:User {name: "Alisa"}), (p:Post {id: 1})
CREATE (a)-[:WROTE]->(p)

MATCH (b:User {name: "Bob"}), (p:Post {id: 2})
CREATE (b)-[:WROTE]->(p)

MATCH (c:User {name: "David"}), (p:Post {id: 3})
CREATE (c)-[:WROTE]->(p)


######################################################################################
Лайки от пользователей к постам:
MATCH (a:User {name: "Alisa"}), (l:Like {like_id: 101}), (p:Post {id: 2})
CREATE (a)-[:GAVE_LIKE]->(l)-[:LIKED_POST]->(p)

MATCH (b:User {name: "Bob"}), (l:Like {like_id: 102}), (p:Post {id: 1})
CREATE (b)-[:GAVE_LIKE]->(l)-[:LIKED_POST]->(p)

MATCH (c:User {name: "David"}), (l:Like {like_id: 103}), (p:Post {id: 1})
CREATE (c)-[:GAVE_LIKE]->(l)-[:LIKED_POST]->(p)

MATCH (a:User {name: "Alisa"}), (l:Like {like_id: 104}), (p:Post {id: 3})
CREATE (a)-[:GAVE_LIKE]->(l)-[:LIKED_POST]->(p)"""
