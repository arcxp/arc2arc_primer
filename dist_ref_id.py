import arc_endpoints
import jmespath
import requests
from jmespath import search


def get_distributor_url(org, dist_id=None) -> str:
    if dist_id:
        return f"https://api.{org}.arcpublishing.com/settings/v1/distributor/{dist_id}"
    return f"https://api.{org}.arcpublishing.com/settings/v1/distributor/"


def find_reference_id(ans):
    # Is there a distributor in the ans?
    try:
        references_distributor = jmespath.search("distributor.reference_id", ans) or ""
    except:
        references_distributor = None
    return references_distributor


def rewrite_reference_id(
    org, to_org, source_dist_id, arc_auth_header_source, arc_auth_header_target
) -> dict:
    dist_source_target_ids = {}
    # figure out what the new distributor id for the target org should be, update in ANS
    # grab the original distributor name so you can look up the same named distributor in target org
    source_distributor = requests.get(
        get_distributor_url(org, source_dist_id), headers=arc_auth_header_source
    )
    source_distributor = source_distributor.json().get("name", "undefined")
    target_distributors = requests.get(
        get_distributor_url(to_org), headers=arc_auth_header_target
    )
    target_distributors = target_distributors.json().get("rows")
    target_dist_id = jmespath.search(
        f"[*] | [?name==`{source_distributor}`].id | [0]", target_distributors
    )
    dist_source_target_ids[source_dist_id] = target_dist_id
    return target_dist_id, dist_source_target_ids


def create_target_distributor_restrictions(
    org, to_org, ans, arc_auth_header_source, arc_auth_header_target, to_website
):
    # This will do as best as can to copy the distributor from the original org into the new org.
    # When restrictions are made to the new org, all are tied to just one website, passed into the script
    # Any more sophisticated manipulation of the restrictions (multiple websites) need to be done manually in the UI
    dist_id = find_reference_id(ans)
    dist_source_target_ids = {}
    new_restr_ids = []
    if dist_id:
        target_dist_id, dist_source_target_ids = rewrite_reference_id(
            org, to_org, dist_id, arc_auth_header_source, arc_auth_header_target
        )
        if not target_dist_id:
            dist_res = requests.get(
                arc_endpoints.get_distributor_url(org, dist_id),
                headers=arc_auth_header_source,
            )
            if dist_res.ok:
                transformed_ans = dist_res.json()

                # if there are restrictions, these have to be created first
                if transformed_ans["restrictions"]:
                    # maintain a list of already registered restrictions. API does not allow multiple restrictions with the same name.
                    all_restrictions = requests.get(
                        arc_endpoints.get_restriction_url(to_org),
                        headers=arc_auth_header_target,
                    )
                    all_restrictions = all_restrictions.json().get("rows")

                    # prepare restriction data for creation of new one in the target organization
                    for restr in transformed_ans["restrictions"]:
                        old_restr_id = restr["id"]
                        restr.pop("id", None)
                        restr.pop("createdBy", None)
                        restr.pop("createdAt", None)
                        restr.pop("modifiedBy", None)
                        restr.pop("modifiedAt", None)
                        for site in restr[
                            "websites"
                        ]:  # WARN won't work when more than 1 website
                            site["siteId"] = to_website

                        # make distributor restriction
                        # might not be able to create the restriction (500 error) if one just like this already exists
                        try:
                            restr_res = requests.post(
                                arc_endpoints.get_restriction_url(to_org),
                                headers=arc_auth_header_target,
                                json=restr,
                            )
                            new_restr_ids.insert(
                                0, {"id": restr_res.json().get("data").get("id")}
                            )
                        except:
                            # find existing restriction by the restriction name.  Use that id.
                            # if this doesn't work the restrictions will end up as an empty array
                            # and you'll have to recreate the restrictions manually in the UI
                            existing_restr_id = jmespath.search(
                                f"[*] | [?name==`{restr['name']}`].id", all_restrictions
                            )
                            if existing_restr_id:
                                new_restr_ids.insert(0, {"id": existing_restr_id[0]})

                transformed_ans = dist_res.json()
                transformed_ans.pop("id", None)
                transformed_ans.pop("organizationId", None)
                transformed_ans.pop("createdAt", None)
                transformed_ans.pop("createdBy", None)
                transformed_ans.pop("modifiedAt", None)
                transformed_ans.pop("modifiedBy", None)
                transformed_ans.pop("organization_id", None)
                transformed_ans.pop("organization", None)
                transformed_ans["restrictions"] = new_restr_ids

                # create or update the restriction in the target org
                dist_res2 = requests.post(
                    arc_endpoints.get_distributor_url(to_org),
                    headers=arc_auth_header_target,
                    json=transformed_ans,
                )
                if not dist_res2.ok:
                    target_dist_id = jmespath.search("context.distributor.id", dist_res2.json())
                else:
                    new_dist = dist_res2.json()
                    target_dist_id = new_dist["data"].get("id")

                if target_dist_id:
                    ans["distributor"]["reference_id"] = target_dist_id
                    dist_source_target_ids[dist_id] = target_dist_id
    return ans, dist_source_target_ids


def create_target_geographic_restrictions(
    org,
    to_org,
    ans,
    arc_auth_header_source,
    arc_auth_header_target,
    dry_run_msg,
    dry_run=False,
):
    geo_restrictions = {}
    if ans.get("content_restrictions", {}).get("geo"):
        restriction_ids = jmespath.search(
            "content_restrictions.geo.restrictions[*].restriction_id", ans
        )
        new_geo_restriction_ids = {}
        new_geo_restrictions = []
        if not dry_run:
            for geo_id in restriction_ids:
                restr_res = requests.get(
                    arc_endpoints.get_geographic_restriction_url(org, geo_id),
                    headers=arc_auth_header_source,
                )
                restr = restr_res.json()
                restr.pop("createdAt", None)
                restr.pop("createdBy", None)
                restr.pop("modifiedBy", None)
                restr.pop("modifiedAt", None)
                orig_restr_id = restr["id"]
                restr.pop("id", None)
                new_geo_id = None
                try:
                    # create a new geo restriction; will fail if one with same name already exists
                    restr_res = requests.post(
                        arc_endpoints.get_geographic_restriction_url(to_org),
                        headers=arc_auth_header_target,
                        json=restr,
                    )
                    new_geo_id = restr_res.json()["id"]
                except:
                    # find existing geo restriction in target org with this name, return that value
                    restr_res = requests.get(
                        arc_endpoints.get_geographic_restriction_url(to_org),
                        headers=arc_auth_header_target,
                        params={"name": restr["name"], "limit": 1},
                    )
                    if restr_res.ok:
                        geo = restr_res.json()
                        new_geo_id = jmespath.search("data | [0] | id", geo)
                finally:
                    new_geo_restrictions.append({"restriction_id": new_geo_id})
                    new_geo_restriction_ids.update({orig_restr_id: new_geo_id})
                    # print(restr_res)
                    # print(restr_res.json())

            ans["content_restrictions"]["geo"]["restrictions"] = new_geo_restrictions
            geo_restrictions = new_geo_restriction_ids
        elif restriction_ids:
            ans["content_restrictions"]["geo"]["restrictions"] = [dry_run_msg]
            geo_restrictions = {restriction_ids[0]: dry_run_msg}
    return ans, geo_restrictions
