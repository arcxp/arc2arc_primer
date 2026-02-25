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


def _fetch_source_distributor(org, dist_id, arc_auth_header_source):
    """Fetch distributor data from source organization.
    
    Args:
        org: Source Arc organization identifier
        dist_id: Distributor ID in source organization
        arc_auth_header_source: Authorization header for source organization
        
    Returns:
        Distributor data dictionary if successful, None otherwise
    """
    try:
        dist_res = requests.get(
            arc_endpoints.get_distributor_url(org, dist_id),
            headers=arc_auth_header_source,
            timeout=30
        )
        if dist_res.ok:
            return dist_res.json()
    except requests.exceptions.RequestException as e:
        print(f"Warning: Could not fetch source distributor {dist_id}: {type(e).__name__}")
    return None


def _get_all_target_restrictions(to_org, arc_auth_header_target):
    """Get all existing restrictions in target organization.
    
    Args:
        to_org: Target Arc organization identifier
        arc_auth_header_target: Authorization header for target organization
        
    Returns:
        List of existing restrictions, empty list if error
    """
    try:
        all_restrictions_res = requests.get(
            arc_endpoints.get_restriction_url(to_org),
            headers=arc_auth_header_target,
            timeout=30
        )
        if all_restrictions_res.ok:
            return all_restrictions_res.json().get("rows", [])
    except requests.exceptions.RequestException as e:
        print(f"Warning: Could not fetch target restrictions: {type(e).__name__}")
    return []


def _prepare_restriction_for_creation(restr, to_website):
    """Clean up restriction data and set target website.
    
    Removes metadata fields that shouldn't be sent when creating a new restriction.
    Updates website IDs to use the target website.
    
    Args:
        restr: Restriction dictionary from source org
        to_website: Target website identifier
        
    Returns:
        Cleaned restriction dictionary ready for creation
    """
    # Remove metadata fields that shouldn't be in creation payload
    restr.pop("id", None)
    restr.pop("createdBy", None)
    restr.pop("createdAt", None)
    restr.pop("modifiedBy", None)
    restr.pop("modifiedAt", None)
    
    # Update website IDs to target website
    # WARN: won't work when more than 1 website
    for site in restr.get("websites", []):
        site["siteId"] = to_website
    
    return restr


def _create_or_find_restriction(restr, to_org, all_restrictions, arc_auth_header_target):
    """Create a new restriction or find existing one by name.
    
    Attempts to create the restriction. If it fails (e.g., duplicate name),
    searches for an existing restriction with the same name.
    
    Args:
        restr: Restriction dictionary ready for creation
        to_org: Target Arc organization identifier
        all_restrictions: List of existing restrictions in target org
        arc_auth_header_target: Authorization header for target organization
        
    Returns:
        Restriction ID if successful, None otherwise
    """
    restriction_name = restr.get("name", "unknown")
    
    # Try to create the restriction
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
            return new_id
    except (requests.exceptions.HTTPError, requests.exceptions.RequestException, KeyError, ValueError, TypeError) as e:
        # Creation failed, try to find existing restriction by name
        print(f"Warning: Could not create restriction '{restriction_name}': {type(e).__name__}")
        existing_restr_id = jmespath.search(
            f"[*] | [?name==`{restriction_name}`].id", all_restrictions
        )
        if existing_restr_id:
            return existing_restr_id[0]
    
    return None


def _create_restrictions(restrictions, to_org, to_website, arc_auth_header_target):
    """Create all restrictions for a distributor in target organization.
    
    Args:
        restrictions: List of restriction dictionaries from source org
        to_org: Target Arc organization identifier
        to_website: Target website identifier
        arc_auth_header_target: Authorization header for target organization
        
    Returns:
        List of restriction ID dictionaries in format [{"id": "..."}]
    """
    if not restrictions:
        return []
    
    # Get all existing restrictions to check for duplicates
    all_restrictions = _get_all_target_restrictions(to_org, arc_auth_header_target)
    
    new_restr_ids = []
    for restr in restrictions:
        # Prepare restriction for creation
        prepared_restr = _prepare_restriction_for_creation(restr.copy(), to_website)
        
        # Create or find restriction
        restriction_id = _create_or_find_restriction(
            prepared_restr, to_org, all_restrictions, arc_auth_header_target
        )
        
        if restriction_id:
            new_restr_ids.insert(0, {"id": restriction_id})
        else:
            print(f"Warning: Could not create or find restriction '{prepared_restr.get('name', 'unknown')}'")
    
    return new_restr_ids


def _prepare_distributor_for_creation(distributor_data, new_restr_ids):
    """Clean up distributor data for creation in target organization.
    
    Removes organization-specific metadata and sets the new restriction IDs.
    
    Args:
        distributor_data: Distributor dictionary from source org
        new_restr_ids: List of restriction ID dictionaries for target org
        
    Returns:
        Cleaned distributor dictionary ready for creation
    """
    # Create a copy to avoid modifying the original
    transformed_ans = distributor_data.copy()
    
    # Remove metadata fields that shouldn't be in creation payload
    transformed_ans.pop("id", None)
    transformed_ans.pop("organizationId", None)
    transformed_ans.pop("createdAt", None)
    transformed_ans.pop("createdBy", None)
    transformed_ans.pop("modifiedAt", None)
    transformed_ans.pop("modifiedBy", None)
    transformed_ans.pop("organization_id", None)
    transformed_ans.pop("organization", None)
    
    # Set the new restriction IDs
    transformed_ans["restrictions"] = new_restr_ids
    
    return transformed_ans


def _create_distributor_in_target(transformed_ans, to_org, arc_auth_header_target):
    """Create distributor in target organization.
    
    Args:
        transformed_ans: Cleaned distributor dictionary ready for creation
        to_org: Target Arc organization identifier
        arc_auth_header_target: Authorization header for target organization
        
    Returns:
        Target distributor ID if successful, None otherwise
    """
    try:
        dist_res = requests.post(
            arc_endpoints.get_distributor_url(to_org),
            headers=arc_auth_header_target,
            json=transformed_ans,
            timeout=30
        )
        
        if dist_res.ok:
            new_dist = dist_res.json()
            return new_dist.get("data", {}).get("id")
        else:
            # Try to extract ID from error response
            try:
                error_data = dist_res.json()
                return jmespath.search("context.distributor.id", error_data)
            except (ValueError, KeyError, TypeError):
                pass
    except requests.exceptions.RequestException as e:
        print(f"Warning: Could not create distributor in target org: {type(e).__name__}")
    
    return None


def create_target_distributor_restrictions(
    org, to_org, ans, arc_auth_header_source, arc_auth_header_target, to_website
):
    """Create or find matching distributor in target org and update ANS.
    
    Attempts to find a distributor with the same name in the target org. If not found,
    creates a new distributor with the same configuration, including restrictions.
    All restrictions are tied to the single website passed in.
    
    This will do as best as can to copy the distributor from the original org into the new org.
    When restrictions are made to the new org, all are tied to just one website, passed into the script.
    Any more sophisticated manipulation of the restrictions (multiple websites) need to be done manually in the UI.
    
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
    dist_id = find_reference_id(ans)
    dist_source_target_ids = {}
    
    if not dist_id:
        return ans, dist_source_target_ids
    
    # Try to find existing distributor in target org by name
    target_dist_id, dist_source_target_ids = rewrite_reference_id(
        org, to_org, dist_id, arc_auth_header_source, arc_auth_header_target
    )
    
    # If distributor doesn't exist in target, create it
    if not target_dist_id:
        # Fetch source distributor data
        distributor_data = _fetch_source_distributor(org, dist_id, arc_auth_header_source)
        if not distributor_data:
            return ans, dist_source_target_ids
        
        # Create restrictions first (they must exist before creating distributor)
        restrictions = distributor_data.get("restrictions", [])
        new_restr_ids = _create_restrictions(
            restrictions, to_org, to_website, arc_auth_header_target
        )
        
        # Prepare distributor data for creation
        transformed_ans = _prepare_distributor_for_creation(distributor_data, new_restr_ids)
        
        # Create distributor in target org
        target_dist_id = _create_distributor_in_target(
            transformed_ans, to_org, arc_auth_header_target
        )
        
        # Update ANS with new distributor ID
        if target_dist_id:
            ans["distributor"]["reference_id"] = target_dist_id
            dist_source_target_ids[dist_id] = target_dist_id
    
    return ans, dist_source_target_ids


def _get_geo_restriction_ids_from_ans(ans):
    """Extract geographic restriction IDs from ANS content_restrictions.geo.

    Args:
        ans: ANS dictionary

    Returns:
        List of restriction_id values if content_restrictions.geo exists, None otherwise.
    """
    if not ans.get("content_restrictions", {}).get("geo"):
        return None
    return jmespath.search(
        "content_restrictions.geo.restrictions[*].restriction_id", ans
    )


def _fetch_source_geographic_restriction(org, geo_id, arc_auth_header_source):
    """Fetch a single geographic restriction from the source organization.

    Args:
        org: Source Arc organization identifier
        geo_id: Geographic restriction ID in source organization
        arc_auth_header_source: Authorization header for source organization

    Returns:
        Restriction dict if successful, None otherwise.
    """
    try:
        restr_res = requests.get(
            arc_endpoints.get_geographic_restriction_url(org, geo_id),
            headers=arc_auth_header_source,
            timeout=30
        )
        restr_res.raise_for_status()
        return restr_res.json()
    except requests.exceptions.RequestException as e:
        print(f"Warning: Could not fetch source geo restriction {geo_id}: {type(e).__name__}")
    return None


def _prepare_geographic_restriction_for_creation(restr):
    """Remove metadata from restriction so it can be used as a creation payload.

    Args:
        restr: Restriction dict from source (will not be mutated; uses copy semantics by caller).

    Returns:
        Tuple of (cleaned_restriction_dict, original_id).
    """
    restr = restr.copy()
    restr.pop("createdAt", None)
    restr.pop("createdBy", None)
    restr.pop("modifiedBy", None)
    restr.pop("modifiedAt", None)
    orig_restr_id = restr.get("id")
    restr.pop("id", None)
    return restr, orig_restr_id


def _create_or_find_geographic_restriction(restr, to_org, arc_auth_header_target):
    """Create a geographic restriction in target org, or find existing one by name.

    Args:
        restr: Restriction dict ready for creation (no id/metadata).
        to_org: Target Arc organization identifier
        arc_auth_header_target: Authorization header for target organization

    Returns:
        New restriction id if created or found, None otherwise.
    """
    restriction_name = restr.get("name", "unknown")
    try:
        restr_res = requests.post(
            arc_endpoints.get_geographic_restriction_url(to_org),
            headers=arc_auth_header_target,
            json=restr,
            timeout=30
        )
        restr_res.raise_for_status()
        restr_data = restr_res.json()
        new_geo_id = restr_data.get("id")
        if new_geo_id:
            return new_geo_id
    except (requests.exceptions.HTTPError, requests.exceptions.RequestException, KeyError, ValueError, TypeError) as e:
        print(f"Warning: Could not create geo restriction '{restriction_name}': {type(e).__name__}")
        try:
            restr_res = requests.get(
                arc_endpoints.get_geographic_restriction_url(to_org),
                headers=arc_auth_header_target,
                params={"name": restr["name"], "limit": 1},
                timeout=30
            )
            if restr_res.ok:
                geo = restr_res.json()
                return jmespath.search("data | [0] | id", geo)
        except (requests.exceptions.RequestException, KeyError, ValueError, TypeError):
            print("Warning: Could not find existing geo restriction")
    return None


def _create_geographic_restrictions_in_target(
    restriction_ids, org, to_org, arc_auth_header_source, arc_auth_header_target
):
    """For each source geo restriction id, fetch, prepare, create-or-find in target.

    Args:
        restriction_ids: List of geographic restriction IDs in source org
        org: Source Arc organization identifier
        to_org: Target Arc organization identifier
        arc_auth_header_source: Authorization header for source organization
        arc_auth_header_target: Authorization header for target organization

    Returns:
        Tuple of (new_geo_restrictions list of {"restriction_id": id}, new_geo_restriction_ids dict).
    """
    new_geo_restrictions = []
    new_geo_restriction_ids = {}
    for geo_id in restriction_ids:
        restr = _fetch_source_geographic_restriction(org, geo_id, arc_auth_header_source)
        if not restr:
            new_geo_restrictions.append({"restriction_id": None})
            new_geo_restriction_ids[geo_id] = None
            continue
        restr, orig_restr_id = _prepare_geographic_restriction_for_creation(restr)
        new_geo_id = _create_or_find_geographic_restriction(
            restr, to_org, arc_auth_header_target
        )
        new_geo_restrictions.append({"restriction_id": new_geo_id})
        new_geo_restriction_ids[orig_restr_id] = new_geo_id
    return new_geo_restrictions, new_geo_restriction_ids


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
    restriction_ids = _get_geo_restriction_ids_from_ans(ans)
    if restriction_ids is None:
        return ans, geo_restrictions

    if not dry_run:
        new_geo_restrictions, geo_restrictions = _create_geographic_restrictions_in_target(
            restriction_ids, org, to_org, arc_auth_header_source, arc_auth_header_target
        )
        ans["content_restrictions"]["geo"]["restrictions"] = new_geo_restrictions
    elif restriction_ids:
        ans["content_restrictions"]["geo"]["restrictions"] = [dry_run_msg]
        geo_restrictions = {restriction_ids[0]: dry_run_msg}

    return ans, geo_restrictions
