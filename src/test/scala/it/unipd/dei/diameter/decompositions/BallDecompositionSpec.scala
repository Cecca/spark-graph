package it.unipd.dei.diameter.decompositions

import org.scalatest._
import BallDecomposition._
import spark.{RDD, SparkContext}
import SparkContext._

class BallDecompositionSpec extends FlatSpec {

  "Function convertInput" should "convert input" in {

    assert( convertInput("0 1 2 3 4") === (0, Seq(1, 2, 3, 4)) )
    assert( convertInput("0") === (0, Seq()) )
    assert( convertInput("0 3") === (0, Seq(3)) )

  }

  "Function sendBall" should "send balls to all neighbours and the node" in {

    val ball = Seq(3,4,6,5,2,8,9,1)
    val data = (0, (Seq(3,4,6,1), ball))

    val expected = List(
      (3, ball),
      (4, ball),
      (6, ball),
      (1, ball),
      (0, ball)
    )

    assert( sendBall(data) === expected )

  }

  "Function reduceBalls" should "return the merge of the balls without duplicates" in {

    assert( reduceBalls(Seq(1,2,3), Seq(5,6,7)) === Seq(1,2,3,5,6,7) )
    assert( reduceBalls(Seq(1,2,3), Seq(1,2,5,6,7)) === Seq(1,2,3,5,6,7) )
    assert( reduceBalls(Seq(1,2,2,3), Seq(5,6,7)) === Seq(1,2,3,5,6,7) )

  }

  "Function removeSelfLoops" should "remove th ID from the ball" in {

    assert( removeSelfLoops(0, Seq(0,1,2,3,4)) === (0, Seq(1,2,3,4)) )
    assert( removeSelfLoops(0, Seq(1,2,3,4)) === (0, Seq(1,2,3,4)) )
    assert( removeSelfLoops(0, Seq(0,1,2,3,0,4)) === (0, Seq(1,2,3,4)) )

  }

  "Function countCardinalities" should "tell the size of a ball" in {

    assert( countCardinalities((0,Seq(1,2,3,4,5,6))) === (0,(6,Seq(1,2,3,4,5,6))) )
    assert( countCardinalities((0,Seq())) === (0,(0,Seq())) )

  }

  "Function sendCardinalities" should
    "send cardinalities to all neighbours in the ball" in {

    assert(
      sendCardinalities(0, (6,Seq(1,2,3,4,5,6))) ===
      List(
        (1,(0,6)), (2,(0,6)), (3,(0,6)), (4,(0,6)),
        (5,(0,6)), (6,(0,6)), (0,(0,6))
      )
    )

  }

  "Function maxCardinality" should "find the maximum cardinality" in {

    assert( maxCardinality((0,2), (1,4)) === (1,4) )
    assert( maxCardinality((5,35), (1,4)) === (5,35) )
    assert( maxCardinality((1,4), (1,4)) === (1,4) )

  }

  it should "order by ID in case of equal cadinality" in {

    assert( maxCardinality((0,2), (1,2)) === (1,2) )

  }

  "Function isCenter" should
    "tell if a node has the maximum ball cardinality among it ball neighbours" in {

    assert( isCenter( (0, Seq((15,32),(0,40),(3,21),(4,2),(6,9))) ) === true )
    assert( isCenter( (0, Seq((15,32),(0,40),(3,41),(4,2),(6,9))) ) === false )
    assert( isCenter( (0, Seq(
        (0,333), (1,17), (2,10), (3,17), (4,10),
        (5,13), (6,6), (7,20), (8,8), (9,57), (10,10), (13,31),
        (14,15), (16,9), (17,13), (19,16), (20,15), (21,65),
        (22,11), (23,17), (24,16), (25,69), (26,68), (27,5),
        (28,13), (29,13), (30,17), (31,23), (32,6), (33,2),
        (34,5), (35,2), (36,11), (38,9), (39,15), (40,44),
        (41,24), (42,2), (44,6), (45,12), (46,5), (47,2),
        (48,22), (49,4), (50,11), (51,7), (52,2), (53,31),
        (54,8), (55,17), (56,78), (57,15), (58,11), (59,19),
        (60,8), (61,3), (62,26), (63,6), (64,7), (65,12), (66,15),
        (67,76), (68,9), (69,10), (70,2), (71,3), (72,24), (73,10),
        (75,14), (76,3), (77,6), (78,9), (79,12), (80,23), (81,3),
        (82,34), (83,7), (84,13), (85,14), (86,6), (87,13), (88,20),
        (89,8), (90,2), (91,8), (92,21), (93,8), (94,22), (95,6), (96,9),
        (97,3), (98,49), (99,13), (100,9), (101,19), (102,6), (103,16),
        (104,32), (105,14), (106,8), (107,1034), (108,13), (109,37),
        (110,5), (111,14), (112,3), (113,40), (115,21), (116,17),
        (117,6), (118,36), (119,62), (120,4), (121,12), (122,63),
        (123,18), (124,4), (125,4), (126,7), (127,16), (128,28),
        (129,7), (130,16), (131,7), (132,16), (133,18), (134,19),
        (135,10), (136,133), (137,16), (138,2), (139,9), (140,11),
        (141,28), (142,43), (143,12), (144,15), (145,2), (146,10),
        (147,6), (148,20), (149,14), (150,11), (151,7), (152,5),
        (153,2), (154,2), (155,3), (156,12), (157,3), (158,25),
        (159,14), (160,2), (161,25), (162,8), (163,6), (164,3),
        (165,11), (166,4), (167,7), (168,11), (169,38), (170,46),
        (171,22), (172,41), (173,12), (174,4), (175,17), (176,14),
        (177,11), (178,13), (179,3), (180,20), (181,10), (182,3),
        (183,2), (184,18), (185,26), (186,44), (187,16), (188,48),
        (189,7), (190,4), (191,3), (192,5), (193,5), (194,19), (195,9),
        (196,13), (197,16), (198,12), (199,47), (200,57), (201,4),
        (202,4), (203,57), (204,22), (205,2), (206,4), (207,3), (208,7),
        (211,30), (212,18), (213,39), (214,17), (216,2), (217,8), (218,9),
        (219,6), (220,4), (221,8), (222,11), (223,27), (224,28), (225,10),
        (226,14), (227,15), (228,3), (229,6), (230,9), (231,21), (232,25),
        (233,2), (234,2), (235,5), (236,37), (237,7), (238,23), (239,59),
        (240,3), (241,2), (242,24), (243,8), (244,2), (245,5), (246,14),
        (247,3), (248,21), (249,24), (250,5), (251,14), (252,65), (253,3),
        (254,17), (255,2), (256,2), (257,18), (258,15), (259,8), (260,8),
        (261,38), (262,4), (263,7), (264,5), (265,27), (266,18), (267,2),
        (268,11), (269,6), (270,4), (271,73), (272,45), (273,9), (274,14),
        (275,10), (276,18), (277,65), (278,10), (279,2), (280,43), (281,16),
        (282,2), (283,5), (284,16), (285,47), (286,2), (288,4), (289,4),
        (290,14), (291,36), (293,3), (294,3), (295,10), (296,7), (297,25),
        (298,11), (299,20), (300,7), (301,3), (302,20), (303,21), (304,55),
        (305,2), (306,9), (307,4), (308,24), (309,10), (310,13), (311,7),
        (312,26), (313,37), (314,13), (315,56), (316,2), (317,7), (318,11),
        (319,8), (320,21), (321,3), (322,72), (323,39), (324,26), (325,39),
        (326,19), (327,4), (328,9), (329,30), (330,16), (331,20), (332,43),
        (333,8), (334,28), (336,3), (337,9), (338,7), (339,27), (340,6),
        (341,12), (342,34), (343,18), (344,9), (345,16), (346,27), (347,7))) )

      === false
    )

  }

  it should "break ties using IDs" in {

    assert( isCenter( (0, Seq((15,40),(0,40),(3,21),(4,2),(6,9))) ) === false )
    assert( isCenter( (15, Seq((15,40),(0,40),(3,21),(4,2),(6,9))) ) === true )
  }

  "Function removeCardinality" should "remove the cardinality from the pair" in {

    assert( removeCardinality((0,(1,2))) == (0,1) )

  }

  "Function sortPair" should "return a new pair with sorted elements" in {

    assert( sortPair((1,2)) === (1,2) )
    assert( sortPair((2,1)) === (1,2) )
    assert( sortPair((1,1)) === (1,1) )

  }

  "Function reduceGraph" should "contract a star to a single node" in {

    System.clearProperty("spark.driver.port")
    val sc = new SparkContext("local", "reduceGraph test")

    val graph = sc.parallelize(Seq( (0, Seq(1,2,3)),
                                    (1, Seq(0)),
                                    (2, Seq(0)),
                                    (3, Seq(0)) ))
    val colors = sc.parallelize(Seq( (0,0),
                                     (1,0),
                                     (2,0),
                                     (3,0) ))
    val reduced = reduceGraph(graph, colors)
//    reduced.collect().foreach(println(_))
    assert( reduced.collect() === Array((0,0)) )

    sc.stop()
  }

  it should "contract a chain with a star to a chain" in {

    System.clearProperty("spark.driver.port")
    val sc = new SparkContext("local", "reduceGraph test")

    val graph = sc.parallelize(Seq( (0, Seq(1,2,3)),
                                    (1, Seq(0)),
                                    (2, Seq(0)),
                                    (3, Seq(0,4)),
                                    (4, Seq(3,5)),
                                    (5, Seq(4))))
    val colors = sc.parallelize(Seq((0,0),
                                    (1,0),
                                    (2,0),
                                    (3,0),
                                    (4,4),
                                    (5,4)))
    val reduced = reduceGraph(graph, colors)
    assert( reduced.collect().sorted === Array((0,0),
                                               (4,4),
                                               (4,0),
                                               (0,4)).sorted )

    sc.stop()
  }

}
