"""
data.gv.at doesn't seem to have a "normal" JSON API anymore,
so I'll need to parse the RDF instead
"""
from datetime import datetime

from rdflib import Graph, RDF, DCAT, DCTERMS, FOAF
from rdflib import Namespace

from meta.meta_db import Record, Resource
from .globals import s

VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")


def get_datagv_metadata(dataset_id):
    url = f"https://www.data.gv.at/api/hub/repo/datasets/{dataset_id}.rdf?locale=de"
    r = s.get(url)
    r.raise_for_status()
    g = Graph()
    g.parse(data=r.content, format="xml")

    dataset_uri = next(g.subjects(RDF.type, DCAT.Dataset))

    def value(subj, pred):
        o = g.value(subj, pred)
        return str(o) if o is not None else None

    pub = g.value(dataset_uri, DCTERMS.publisher)
    contact_point = g.value(dataset_uri, DCAT.contactPoint)
    meta_obj = Record(
        id=dataset_id,
        title=value(dataset_uri, DCTERMS.title),
        notes=value(dataset_uri, DCTERMS.description),
        tags=[str(o) for o in g.objects(dataset_uri, DCAT.keyword)],
        metadata_created=value(dataset_uri, DCTERMS.issued),
        metadata_modified=value(dataset_uri, DCTERMS.modified),
        license_url=value(dataset_uri, DCTERMS.license),
        publisher=value(pub, FOAF.name),
        maintainer=value(contact_point, VCARD.fn),
        metadata_linkage=None,
        geographic_toponym=value(dataset_uri, DCTERMS.spatial),
        api_data={}
    )

    resources: list[Resource] = []
    for dist in g.objects(dataset_uri, DCAT.distribution):
        r = Resource(
            id=str(g.value(dist, DCTERMS.identifier)).split("/")[-1],
            record=meta_obj,
            format=value(dist, DCTERMS.format),
            name=value(dist, DCTERMS.title),
            url=value(dist, DCAT.accessURL),
            mimetype=value(dist, DCAT.mediaType),
            last_fetched=datetime.now(),
        )
        resources.append(r)
    resources.sort(key=lambda r: r.name)
    return meta_obj, resources


#
# BASE_URL = "https://www.data.gv.at/katalog"
# API_BASE = BASE_URL + "/api/3/action/package_show?id="
#
#
# def get_metadata(package_id):
#     url = API_BASE + package_id
#     print(url)
#     r = s.get(url)
#     r.raise_for_status()
#     assert r.json()["success"]
#     data = r.json()["result"]
#     return data

if __name__ == '__main__':
    print(get_datagv_metadata("72b98e17-0839-455e-9992-86f93a0cdca2"))
