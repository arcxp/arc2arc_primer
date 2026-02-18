def draft_find_revision_url(org, arc_id) -> str:
    """Generate URL to find the published revision for a story.
    
    Args:
        org: Arc organization identifier
        arc_id: Arc ID of the story
        
    Returns:
        URL string for the draft revision endpoint
    """
    return f"https://api.{org}.arcpublishing.com/draft/v1/story/{arc_id}"


def draft_get_story_url(org, arc_id, revision_id) -> str:
    """Generate URL to retrieve a specific story revision.
    
    Args:
        org: Arc organization identifier
        arc_id: Arc ID of the story
        revision_id: Revision ID of the story
        
    Returns:
        URL string for the story revision endpoint
    """
    return f"https://api.{org}.arcpublishing.com/draft/v1/story/{arc_id}/revision/{revision_id}"


def draft_get_circulations_url(org, arc_id) -> str:
    """Generate URL to retrieve story circulations.
    
    Args:
        org: Arc organization identifier
        arc_id: Arc ID of the story
        
    Returns:
        URL string for the story circulations endpoint
    """
    return f"https://api.{org}.arcpublishing.com/draft/v1/story/{arc_id}/circulation"


def mc_create_ans_url(org) -> str:
    """Generate URL for Migration Center ANS content creation endpoint.
    
    Args:
        org: Arc organization identifier
        
    Returns:
        URL string for the Migration Center ANS endpoint
    """
    return f"https://api.{org}.arcpublishing.com/migrations/v3/content/ans"


def get_galleries_url(org, arc_id) -> str:
    """Generate URL to retrieve a gallery by Arc ID.
    
    Args:
        org: Arc organization identifier
        arc_id: Arc ID of the gallery
        
    Returns:
        URL string for the gallery endpoint
    """
    return f"https://api.{org}.arcpublishing.com/photo/api/v2/galleries/{arc_id}/"


def get_photo_url(org, arc_id) -> str:
    """Generate URL to retrieve a photo/image by Arc ID.
    
    Args:
        org: Arc organization identifier
        arc_id: Arc ID of the photo/image
        
    Returns:
        URL string for the photo endpoint
    """
    return f"https://api.{org}.arcpublishing.com/photo/api/v2/photos/{arc_id}/"


def get_author_url(org, version="v1") -> str:
    """Generate URL for author service endpoint.
    
    Args:
        org: Arc organization identifier
        version: API version (default: "v1")
        
    Returns:
        URL string for the author service endpoint
    """
    return f"https://api.{org}.arcpublishing.com/author/{version}/author-service/"


def get_all_authors_url(org) -> str:
    """Generate URL to retrieve all authors.
    
    Args:
        org: Arc organization identifier
        
    Returns:
        URL string for the all authors endpoint
    """
    return f"https://api.{org}.arcpublishing.com/author/v1/"


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


def get_restriction_url(org, restr_id=None) -> str:
    """Generate URL for restriction endpoint (single restriction or list).
    
    Args:
        org: Arc organization identifier
        restr_id: Optional restriction ID. If provided, returns URL for specific restriction.
                  If None, returns URL for list of all restrictions.
        
    Returns:
        URL string for the restriction endpoint
    """
    if restr_id:
        return f"https://api.{org}.arcpublishing.com/settings/v1/restriction/{restr_id}"
    return f"https://api.{org}.arcpublishing.com/settings/v1/restriction/"


def get_geographic_restriction_url(org, restr_id=None) -> str:
    """Generate URL for geographic restriction endpoint (single restriction or list).
    
    Args:
        org: Arc organization identifier
        restr_id: Optional geographic restriction ID. If provided, returns URL for specific restriction.
                  If None, returns URL for list of all geographic restrictions.
        
    Returns:
        URL string for the geographic restriction endpoint
    """
    if restr_id:
        return f"https://api.{org}.arcpublishing.com/settings/v1/geo-restriction/{restr_id}"
    return f"https://api.{org}.arcpublishing.com/settings/v1/geo-restriction/"


def get_video_url(org, env) -> str:
    """Generate URL to find a video by UUID.
    
    Args:
        org: Arc organization identifier
        env: Environment identifier (e.g., "prod", "sandbox")
        
    Returns:
        URL string for the video find endpoint
    """
    return (
        f"https://{org}-{env}.video-api.arcpublishing.com/api/v1/ansvideos/findByUuid"
    )


def ans_validation_url(org, version=None) -> str:
    """Generate URL for ANS validation endpoint.
    
    Args:
        org: Arc organization identifier
        version: Optional ANS version. If None, uses ANS_VERSION_STANDARD from arc_constants.
        
    Returns:
        URL string for the ANS validation endpoint
    """
    if version is None:
        import arc_constants
        version = arc_constants.ANS_VERSION_STANDARD
    return f"https://api.{org}.arcpublishing.com/ans/validate/{version}"


def get_story_redirects_url(org, arc_id, website, redirect_url=False) -> str:
    """Generate URL for story redirect endpoints.
    
    Args:
        org: Arc organization identifier
        arc_id: Arc ID of the story
        website: Website identifier
        redirect_url: If True, returns URL to create/get a specific redirect by URL.
                      If False, returns URL to get all redirects for a story.
        
    Returns:
        URL string for the story redirect endpoint
    """
    if redirect_url:
        return f"https://api.{org}.arcpublishing.com/draft/v1/redirect/{website}/{redirect_url}/"
    return f"https://api.{org}.arcpublishing.com/draft/v1/story/{arc_id}/redirect/{website}"


def get_lightbox_url(org, lightbox_id=None, photos=False) -> str:
    """Generate URL for lightbox endpoints.
    
    Args:
        org: Arc organization identifier
        lightbox_id: Optional lightbox ID. If None, returns URL for list of all lightboxes.
        photos: If True and lightbox_id provided, returns URL for lightbox photos endpoint.
        
    Returns:
        URL string for the lightbox endpoint
    """
    if lightbox_id and photos:
        return f"https://api.{org}.arcpublishing.com/photo/api/v2/lightboxes/{lightbox_id}/photos"
    elif lightbox_id:
        return (
            f"https://api.{org}.arcpublishing.com/photo/api/v2/lightboxes/{lightbox_id}"
        )
    return f"https://api.{org}.arcpublishing.com/photo/api/v2/lightboxes/"


def get_collection_url(org, collection_id=None) -> str:
    """Generate URL for collection endpoints.
    
    Args:
        org: Arc organization identifier
        collection_id: Optional collection ID. If provided, returns URL for specific collection.
                        If None, returns URL for list of all collections.
        
    Returns:
        URL string for the collection endpoint
    """
    if collection_id:
        return f"https://api.{org}.arcpublishing.com/websked/collections/v1/collections/{collection_id}"
    return f"https://api.{org}.arcpublishing.com/websked/collections/v1/collections/"
