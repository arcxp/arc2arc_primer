def draft_find_revision_url(org, arc_id) -> str:
    return f"https://api.{org}.arcpublishing.com/draft/v1/story/{arc_id}"


def draft_get_story_url(org, arc_id, revision_id) -> str:
    return f"https://api.{org}.arcpublishing.com/draft/v1/story/{arc_id}/revision/{revision_id}"


def draft_get_circulations_url(org, arc_id) -> str:
    return f"https://api.{org}.arcpublishing.com/draft/v1/story/{arc_id}/circulation"


def mc_create_ans_url(org) -> str:
    return f"https://api.{org}.arcpublishing.com/migrations/v3/content/ans"


def get_galleries_url(org, arc_id) -> str:
    return f"https://api.{org}.arcpublishing.com/photo/api/v2/galleries/{arc_id}/"


def get_photo_url(org, arc_id) -> str:
    return f"https://api.{org}.arcpublishing.com/photo/api/v2/photos/{arc_id}/"


def get_author_url(org, version="v1") -> str:
    return f"https://api.{org}.arcpublishing.com/author/{version}/author-service/"


def get_all_authors_url(org) -> str:
    return f"https://api.{org}.arcpublishing.com/author/v1/"


def get_distributor_url(org, dist_id=None) -> str:
    if dist_id:
        return f"https://api.{org}.arcpublishing.com/settings/v1/distributor/{dist_id}"
    return f"https://api.{org}.arcpublishing.com/settings/v1/distributor/"


def get_restriction_url(org, restr_id=None) -> str:
    if restr_id:
        return f"https://api.{org}.arcpublishing.com/settings/v1/restriction/{restr_id}"
    return f"https://api.{org}.arcpublishing.com/settings/v1/restriction/"


def get_geographic_restriction_url(org, restr_id=None) -> str:
    if restr_id:
        return f"https://api.{org}.arcpublishing.com/settings/v1/geo-restriction/{restr_id}"
    return f"https://api.{org}.arcpublishing.com/settings/v1/geo-restriction/"


def get_video_url(org, env) -> str:
    return (
        f"https://{org}-{env}.video-api.arcpublishing.com/api/v1/ansvideos/findByUuid"
    )


def ans_validation_url(org, version="0.10.9") -> str:
    return f"https://api.{org}.arcpublishing.com/ans/validate/{version}"


def get_story_redirects_url(org, arc_id, website, redirect_url=False) -> str:
    if redirect_url:
        return f"https://api.{org}.arcpublishing.com/draft/v1/redirect/{website}/{redirect_url}/"
    return f"https://api.{org}.arcpublishing.com/draft/v1/story/{arc_id}/redirect/{website}"


def get_lightbox_url(org, lightbox_id=None, photos=False) -> str:
    if lightbox_id and photos:
        return f"https://api.{org}.arcpublishing.com/photo/api/v2/lightboxes/{lightbox_id}/photos"
    elif lightbox_id:
        return (
            f"https://api.{org}.arcpublishing.com/photo/api/v2/lightboxes/{lightbox_id}"
        )
    return f"https://api.{org}.arcpublishing.com/photo/api/v2/lightboxes/"


def get_collection_url(org, collection_id=None) -> str:
    if collection_id:
        return f"https://api.{org}.arcpublishing.com/websked/collections/v1/collections/{collection_id}"
    return f"https://api.{org}.arcpublishing.com/websked/collections/v1/collections/"
