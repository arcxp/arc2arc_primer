import arc_endpoints
import jmespath
import requests


def get_distributor_url(org, dist_id=None) -> str:
    """Generate URL for distributor endpoint (single distributor or list).
    
    Args:
        org: Arc organization identifier
        dist_id: Optional distributor ID. If provided, returns URL for specific distributor.
                 If None, returns URL for list of all distributors.
        
    Returns:
        URL string for the distributor endpoint
    """
    if dist_id:
        return f"https://api.{org}.arcpublishing.com/settings/v1/distributor/{dist_id}"
    return f"https://api.{org}.arcpublishing.com/settings/v1/distributor/"


def find_reference_id(ans):
    """Extract distributor reference ID from ANS.
    
    Args:
        ans: ANS dictionary to search for distributor reference
        
    Returns:
        Distributor reference ID string if found, None otherwise
    """
    # Is there a distributor in the ans?
    try:
        references_distributor = jmespath.search("distributor.reference_id", ans) or ""
    except (KeyError, TypeError, AttributeError):
        # ANS structure doesn't have distributor or distributor.reference_id
        references_distributor = None
    except Exception as e:
        # Unexpected error - log it but don't crash
        print(f"Warning: Unexpected error finding distributor reference: {e}")
        references_distributor = None
    return references_distributor


def rewrite_reference_id(
    org, to_org, source_dist_id, arc_auth_header_source, arc_auth_header_target
) -> dict:
    """Find matching distributor in target org by name and return mapping.
    
    Looks up the source distributor by ID, finds a distributor with the same name
    in the target org, and returns a mapping of source to target distributor IDs.
    
    Args:
        org: Source Arc organization identifier
        to_org: Target Arc organization identifier
        source_dist_id: Distributor ID in source organization
        arc_auth_header_source: Authorization header for source organization
        arc_auth_header_target: Authorization header for target organization
        
    Returns:
        Tuple of (target_dist_id, dist_source_target_ids dict mapping source to target IDs)
    """
    dist_source_target_ids = {}
    # figure out what the new distributor id for the target org should be, update in ANS
    # grab the original distributor name so you can look up the same named distributor in target org
    source_distributor = requests.get(
        get_distributor_url(org, source_dist_id), headers=arc_auth_header_source, timeout=30
    )
    source_distributor = source_distributor.json().get("name", "undefined")
    target_distributors = requests.get(
        get_distributor_url(to_org), headers=arc_auth_header_target, timeout=30
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
    """Create or find matching distributor in target org and update ANS.
    
    Attempts to find a distributor with the same name in the target org. If not found,
    creates a new distributor with the same configuration, including restrictions.
    All restrictions are tied to the single website passed in.
    
    Args:
        org: Source Arc organization identifier
        to_org: Target Arc organization identifier
        ans: ANS dictionary to update with new distributor reference
        arc_auth_header_source: Authorization header for source organization
        arc_auth_header_target: Authorization header for target organization
        to_website: Target website identifier for restrictions
        
    Returns:
        Tuple of (updated ans dict, dist_source_target_ids dict mapping source to target IDs)
    """
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
                timeout=30
            )
            if dist_res.ok:
                transformed_ans = dist_res.json()

                # if there are restrictions, these have to be created first
                if transformed_ans["restrictions"]:
                    # maintain a list of already registered restrictions. API does not allow multiple restrictions with the same name.
                    all_restrictions = requests.get(
                        arc_endpoints.get_restriction_url(to_org),
                        headers=arc_auth_header_target,
                        timeout=30
                    )
                    all_restrictions = all_restrictions.json().get("rows")

                    # prepare restriction data for creation of new one in the target organization
                    for restr in transformed_ans["restrictions"]:
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
                                timeout=30
                            )
                            restr_res.raise_for_status()
                            restr_data = restr_res.json()
                            new_id = restr_data.get("data", {}).get("id")
                            if new_id:
                                new_restr_ids.insert(0, {"id": new_id})
                        except (requests.exceptions.HTTPError, requests.exceptions.RequestException, KeyError, ValueError, TypeError) as e:
                            # find existing restriction by the restriction name.  Use that id.
                            # if this doesn't work the restrictions will end up as an empty array
                            # and you'll have to recreate the restrictions manually in the UI
                            print(f"Warning: Could not create restriction '{restr.get('name', 'unknown')}': {type(e).__name__}")
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
                    timeout=30
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
    """Create or find matching geographic restrictions in target org and update ANS.
    
    For each geographic restriction in the source ANS, attempts to find a restriction
    with the same name in the target org. If not found, creates a new restriction.
    
    Args:
        org: Source Arc organization identifier
        to_org: Target Arc organization identifier
        ans: ANS dictionary to update with new geographic restriction references
        arc_auth_header_source: Authorization header for source organization
        arc_auth_header_target: Authorization header for target organization
        dry_run_msg: Message to use for restrictions during dry run
        dry_run: If True, does not create restrictions, only updates with dry_run_msg
        
    Returns:
        Tuple of (updated ans dict, geo_restrictions dict mapping source to target restriction IDs)
    """
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
                    timeout=30
                )
                restr_res.raise_for_status()
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
                        timeout=30
                    )
                    restr_res.raise_for_status()
                    restr_data = restr_res.json()
                    new_geo_id = restr_data.get("id")
                except (requests.exceptions.HTTPError, requests.exceptions.RequestException, KeyError, ValueError, TypeError) as e:
                    # find existing geo restriction in target org with this name, return that value
                    print(f"Warning: Could not create geo restriction '{restr.get('name', 'unknown')}': {type(e).__name__}")
                    try:
                        restr_res = requests.get(
                            arc_endpoints.get_geographic_restriction_url(to_org),
                            headers=arc_auth_header_target,
                            params={"name": restr["name"], "limit": 1},
                            timeout=30
                        )
                        if restr_res.ok:
                            geo = restr_res.json()
                            new_geo_id = jmespath.search("data | [0] | id", geo)
                    except (requests.exceptions.RequestException, KeyError, ValueError, TypeError) as e2:
                        print(f"Warning: Could not find existing geo restriction: {type(e2).__name__}")
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
