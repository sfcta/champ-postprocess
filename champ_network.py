import re
from itertools import chain, pairwise, product, repeat

import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
from igraph import Graph


def load_champ_network(
    champ_links_gis_filepath: str, champ_nodes_gis_filepath: str
) -> nx.DiGraph:
    champ_links = gpd.read_file(champ_links_gis_filepath)
    champ_nodes = gpd.read_file(champ_nodes_gis_filepath)
    # allows loading of edges and vertices in one function
    # TODO if igraph is not needed elsewhere, load using networkx
    g = Graph.DataFrame(  # igraph Graph
        champ_links, directed=True, vertices=champ_nodes, use_vids=False
    )
    DG = g.to_networkx()  # networkx DiGraph
    return DG


def _replace_street_type_abbrs(type: str) -> str:
    # TODO very incomplete
    type = re.sub("^AV$", "AVE", type.upper())
    type = re.sub("^WAY$", "WY", type)
    return type


def _replace_specific_street_name_abbrs(name: str) -> str:
    name = re.sub(r"\bMT\b", "MOUNT", name.upper())
    name = re.sub(r"\bPT\b", "POINT", name)
    return name


def is_match_specific_street_name(
    name1: str, name2: str, replace_abbrs: bool = True, fuzzy: bool = False
):
    """Check if two specific street names (e.g. 'Market' for 'Market St') match

    Parameters
    ----------
    name1 : str
        _description_
    name2 : str
        _description_
    fuzzy : bool, optional
        _description_, by default False

    Returns
    -------
    _type_
        _description_
    """
    name1 = name1.upper()
    name2 = name2.upper()
    if replace_abbrs:
        name1 = _replace_specific_street_name_abbrs(name1)
        name2 = _replace_specific_street_name_abbrs(name2)
    if fuzzy:
        # already handled below when removing all spaces
        # # remove beginning/trailing spaces
        # name1 = name1.strip()
        # name2 = name2.strip()
        # remove cardinal directions in the specific street name
        nsew = "NORTH|SOUTH|EAST|WEST"
        pattern_str = rf"^({nsew})|({nsew})$"
        # remove all spaces
        pattern_str += "| "
        # remove all punctuation
        pattern_str += r"|[^\w\d\s]"
        pattern = re.compile(pattern_str)
        name1 = re.sub(pattern, "", name1)
        name2 = re.sub(pattern, "", name2)
    return name1 == name2


def is_match_street_names(
    name1, type1, name2, type2, fuzzy: bool = False
) -> bool:
    """Check if street 1 and 2 matches by name (e.g. Market) and type (e.g. St)

    Parameters
    ----------
    name1 : str | None | np.nan
        _description_
    type1 : str | None | np.nan
        _description_
    name2 : str | None | np.nan
        _description_
    type2 : str | None | np.nan
        _description_
    fuzzy : bool, optional
        _description_, by default False

    Returns
    -------
    bool
        whether street 1 and street 2 matches
    """
    # TODO? CountDracula also checks for matches with spaces removed in names
    # TODO fuzzy matching for street type (abbreviations, full name)
    if pd.isna(name1) or pd.isna(name2):
        return False
    elif pd.isna(type1) or pd.isna(type2):
        return is_match_specific_street_name(
            name1, name2, replace_abbrs=True, fuzzy=fuzzy
        )
    else:
        type1 = _replace_street_type_abbrs(type1.upper())
        type2 = _replace_street_type_abbrs(type2.upper())
        return (
            is_match_specific_street_name(
                name1, name2, replace_abbrs=True, fuzzy=fuzzy
            )
            and type1 == type2
        )


def is_match_edge_name(
    champ_digraph: nx.DiGraph,
    edge_nodes: tuple[int, int],
    name: str,
    type: str,
    fuzzy: bool = False,
) -> bool:
    """Checks to see if the edge matches the street name and type

    Parameters
    ----------
    champ_digraph : nx.DiGraph
        _description_
    edge_nodes : tuple[int, int]
        _description_
    name : str
        _description_
    type : str
        _description_
    fuzzy : bool, optional
        _description_, by default False

    Returns
    -------
    bool
        _description_
    """
    edge = champ_digraph.edges[edge_nodes]
    edge_name = edge["STREETNAME"]
    edge_type = edge["TYPE"]
    return is_match_street_names(edge_name, edge_type, name, type, fuzzy=fuzzy)


def find_edges_from_streetnames(
    champ_digraph: nx.DiGraph,
    primary_street_name: str,
    primary_street_type: str,
    cross_street_1_name: str,
    cross_street_1_type: str,
    cross_street_2_name: str,
    cross_street_2_type: str,
):
    """Search for a CHAMP link on primary_street between cross_street 1 and 2


    Logic flow (street name here refers to both street name and type):
    0. edges_found = []
    1. Search for all edges that matches the primary street name.
    2. For the two end-nodes of each matching edge, check all their in and out
       edges to see if their names matches the cross streets'.
    3. If both end-nodes of an edge intersects with the two cross streets, then
       append that segment to edges_found.
    4. If no edges satisfy the condition in 3, look up the list of intersections
       that intersect with each cross street. Do a shortest path (by actual
       distance) routing between them, and only keep this shortest path if each
       edge in the shortest path matches the primary street name. Add these to
       edges_found.

    TODO Might want to do an exact (not fuzzy) match for street names first,
    and only do the fuzzy match (can even do this in stages) if there isn't
    an exact match. (Though at what point would using a geocoder directly be
    more efficient?)

    TODO Known limitations:
    - if a road between two intersections is composed of a single link in one
      direction but multiple links in the other direction, this logic only
      returns the edge in the direction with the single link. This applies to,
      e.g. Idora between Garcia and Laguna Honda.

    Parameters
    ----------
    primary_street_name : str
        e.g. Market
    primary_street_type : str
        e.g. St
    cross_street_1_name : str
        _description_
    cross_street_1_type : str
        _description_
    cross_street_2_name : str
        _description_
    cross_street_2_type : str
        _description_

    Returns
    -------
    _type_
        _description_

    Raises
    ------
    NotImplementedError
        _description_
    """
    cross1_nodes = []  # nodes where primary st and cross st 1 meet
    cross2_nodes = []  # nodes where primary st and cross st 2 meet
    edges_found = []
    # check each edge to see if its name & type match the primary street's
    for primary_st_node1, primary_st_node2 in champ_digraph.edges():
        # if the edge's name & type match the primary street's
        if is_match_edge_name(
            champ_digraph,
            (primary_st_node1, primary_st_node2),
            primary_street_name,
            primary_street_type,
            fuzzy=True,
        ):
            cross_street_1_found = False
            cross_street_2_found = False
            # check if each in-coming/out-going edge (of the end-vertices of
            # the found primary street edge/link) match the cross streets'
            # name and type
            # (since the CHAMP network is undirected,
            # in and out nodes need to be handled separately)
            # cross_node = the node of the intersection
            # cross_st_nodes = the end-notes of the cross street
            for cross_node, cross_st_endnodes in chain(
                zip(
                    repeat(primary_st_node1),
                    champ_digraph.in_edges(primary_st_node1),
                ),
                zip(
                    repeat(primary_st_node1),
                    champ_digraph.out_edges(primary_st_node1),
                ),
                zip(
                    repeat(primary_st_node2),
                    champ_digraph.in_edges(primary_st_node2),
                ),
                zip(
                    repeat(primary_st_node2),
                    champ_digraph.out_edges(primary_st_node2),
                ),
            ):
                if is_match_edge_name(
                    champ_digraph,
                    cross_st_endnodes,
                    cross_street_1_name,
                    cross_street_1_type,
                    fuzzy=True,
                ):
                    cross_street_1_found = True
                    cross1_nodes.append(cross_node)
                if is_match_edge_name(
                    champ_digraph,
                    cross_st_endnodes,
                    cross_street_2_name,
                    cross_street_2_type,
                    fuzzy=True,
                ):
                    cross_street_2_found = True
                    cross2_nodes.append(cross_node)
            # TODO unclear if this might happen in real life, but might want to
            # enforce that the each end-node matches a different cross street
            if cross_street_1_found and cross_street_2_found:
                # this particular street segment matches a CHAMP link
                edges_found.append([primary_st_node1, primary_st_node2])
    # N.B. if all of the primary and cross streets are bidirectional,
    # len(cross1_nodes) == len(cross2_nodes) == 16
    if edges_found:  # if not empty
        # N.B. len(edges_found) == 2 if the street segment is bidirectional,
        # otherwise len(edges_found) == 1 if it's one-way
        return edges_found
    # if edges_found is empty but cross1/2_nodes both not empty
    elif cross1_nodes and cross2_nodes:
        # This particular street segment is not a CHAMP link,
        # but the nodes for the two intersections are found,
        # which means that the street segment spans multiple CHAMP links.
        # So we just do a shortest routing (weighted by distance of each edge)
        # between the two nodes and return that as edges_found
        cross1_nodes = set(cross1_nodes)
        cross2_nodes = set(cross2_nodes)
        # TODO issue: this can give a ton of routing options if
        # len(cross1/2_nodes) are not 1
        for crossnodes in product(cross1_nodes, cross2_nodes):
            # check both directions since champ_digraph is bidirectional
            # and there could be one-ways
            for xn1, xn2 in (crossnodes, reversed(crossnodes)):
                shortest_path = nx.shortest_path(
                    champ_digraph, xn1, xn2, weight="DISTANCE"
                )
                pass
                if all(
                    is_match_edge_name(
                        champ_digraph,
                        edge_nodes,
                        primary_street_name,
                        primary_street_type,
                        fuzzy=True,
                    )
                    for edge_nodes in pairwise(shortest_path)
                ):
                    edges_found.append(shortest_path)
        return edges_found
    else:
        return []  # == edges_found
