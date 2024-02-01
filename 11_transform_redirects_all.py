import argparse

import arc_endpoints
import jmespath
import requests


class Arc2ArcRedirects:
    """
    Usage: Query Content Api using a `q=type:redirect`, to return all document or vanity redirects registered in Arc.
    For every result, determine what the redirect's destination object is and attempt to recreate it in the target organization.

    In 01_transform_story.py and 02_transform_story_to_sandbox.py these transform scripts contain functionality that
    creates document level redirects for a story.

    However, there's no way to get redirects "anonymously" from the API for videos and galleries;
    you must already know the redirect url.

    You may query redirects in CAPI by type:redirect and you will get all that exist, but you won't know from the responses
    explicity what kind of object the redirect is for.

    In most implementations of redirects for videos and galleries, the redirect url contains a path segment that is
    explicitly used only for videos and galleries.

    So, to take advantage of this and make it possible that this script could work for videos and galleries, the script
    accepts a passed in value, looks for it in the redirect url and lets that inform what kind of object the redirect is for.


    --from-org devtraining  --to-org cetest --source-website arcdevtraining  --target-website cetest  --from-token devtraining prod token  --to-token cetest prod token  --dry-run 1  --test-run 1

    --from-org devtraining  --to-org cetest  --source-website wdabc  --target-website cetest  --from-token devtraining prod token  --to-token cetest prod token  --dry-run 1 --test-run 101  --video-urlstring /video  --gallery-urlstring /gallery
    """
    def __init__(
        self,
        from_org,
        to_org,
        source_website,
        target_website,
        video_urlstring,
        gallery_urlstring,
        source_auth,
        target_auth,
        dry_run,
        test_run,
    ):
        self.dry_run = bool(int(dry_run))
        self.test_run = int(test_run)
        self.arc_auth_header_source = source_auth
        self.arc_auth_header_target = target_auth
        self.from_org = from_org
        self.to_org = to_org
        self.source_website = source_website
        self.target_website = target_website
        self.video_urlstring = video_urlstring
        self.gallery_urlstring = gallery_urlstring
        self.ans = {}
        self.message = ""
        self.scrollId = None
        self.searchFrom = None
        self.runcount = 0
        self.params = {
            "website": self.source_website,
            "q": "type:redirect",
            "size": 100,
            "scrollId": self.scrollId,  # used when search endpoint is /scan
            "from": self.searchFrom,  # used when search endpoint is /search
        }

    def doit(self):
        all_org_redirects = self.query_redirects()
        redirects = (
            jmespath.search(
                "content_elements[*].{id: _id, arc_url: redirect_url, redirect: canonical_url}",
                all_org_redirects,
                jmespath.Options(dict_cls=dict),
            )
            or []
        )

        # Figure out what kind of object this redirect is for, so you can run the correct endpoint
        for index, item in enumerate(redirects):
            self.runcount += 1
            if len(self.video_urlstring) and self.video_urlstring in item["redirect"]:
                print("video", item)
                redirect_type = "video"
            elif (
                len(self.gallery_urlstring)
                and self.gallery_urlstring in item["redirect"]
            ):
                print("gallery", item)
                redirect_type = "gallery"
            elif "vanity_redirect" in item["id"]:
                # if vanity redirect doesn't use the urlstring then not sure how to tell the object, log it
                print("???", item)
                # but for now will treat as video redirect
                redirect_type = "video"
            else:
                print("story", item)
                redirect_type = "story"

            # attempt to create the redirects
            # 200: redirect was created successfully
            # 400: maybe redirect already exists in this org,
            # 404: maybe the object type is wrong, e.g. you sent a video to the story endpoint, etc
            if not self.dry_run:
                if redirect_type == "story":
                    try:
                        arc_id = item["id"].split("_")[0]
                        redirect_story_res = requests.post(
                            arc_endpoints.get_story_redirects_url(
                                self.to_org,
                                arc_id,
                                self.target_website,
                                item["redirect"],
                            ),
                            headers=self.arc_auth_header_target,
                            json={"document_id": arc_id},
                        )
                        print("story", redirect_story_res, redirect_story_res.text)
                    except Exception as e:
                        print("story", e)

            if bool(self.test_run) and self.runcount >= self.test_run:
                self.searchFrom = self.scrollId = None
                break

        if all_org_redirects.get("content_elements") and (
            self.searchFrom or self.scrollId
        ):
            self.doit()

    def query_redirects(self):
        print(self.searchFrom, self.scrollId)
        self.params["from"] = self.searchFrom
        self.params["scrollId"] = self.scrollId
        try:
            redirects_res = requests.get(
                f"https://api.{self.from_org}.arcpublishing.com/content/v4/scan",
                headers=self.arc_auth_header_source,
                params=self.params,
            )
            all_org_redirects = redirects_res.json()
        except Exception as e:
            print("exception", redirects_res, redirects_res.text, e)
            all_org_redirects = {}

        scroll_or_from = all_org_redirects.get("next", None)

        if all_org_redirects.get("content_elements", []):
            if scroll_or_from and isinstance(scroll_or_from, int):
                self.searchFrom = scroll_or_from
            elif scroll_or_from:
                self.scrollId = scroll_or_from
        else:
            self.searchFrom = self.scrollId = None
        return all_org_redirects


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--from-org",
        dest="org",
        required=True,
        default="",
        help="source organization id value; org for production or sandbox.org for sandbox'",
    )
    parser.add_argument(
        "--to-org",
        dest="to_org",
        required=True,
        default="",
        help="target organization id value; org for production or sandbox.org for sandbox'",
    )
    parser.add_argument(
        "--from-token",
        dest="from_token",
        required=True,
        default="",
        help="source organization bearer token; production environment'",
    )
    parser.add_argument(
        "--to-token",
        dest="to_token",
        required=True,
        default="",
        help="target organization bearer token; production environment'",
    )
    parser.add_argument(
        "--source-website",
        dest="source_website",
        required=True,
        default="",
        help="source organization website'",
    )
    parser.add_argument(
        "--target-website",
        dest="target_website",
        required=True,
        default="",
        help="target organization website'",
    )
    parser.add_argument(
        "--video-urlstring",
        dest="video_urlstring",
        required=False,
        default="",
        help="By default redirects are assumed to be for story objects. This string when found in a url means the object behind the redirect is a video. Will be seen in Url Service and the Pagebuilder resolver regex, e.g. /video",
    )
    parser.add_argument(
        "--gallery-urlstring",
        dest="gallery_urlstring",
        required=False,
        default="",
        help="By default redirects are assumed to be for story objects. This string when found in a url means the object behind the redirect is a gallery. Will be seen in Url Service and the Pagebuilder resolver regex, e.g. /gallery",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        required=False,
        default=1,
        help="Set this to 1 to test the results of transforming a redirect. The redirect will not actually post to the target org.",
    )
    parser.add_argument(
        "--test-run",
        dest="test_run",
        required=False,
        default=5,
        help="Stop processing after this number of loop iterations. When used, must be paired with dry_run 0 (False)",
    )
    args = parser.parse_args()

    arc_auth_header_source = {"Authorization": f"Bearer {args.from_token}"}
    arc_auth_header_target = {"Authorization": f"Bearer {args.to_token}"}

    Arc2ArcRedirects(
        from_org=args.org,
        to_org=args.to_org,
        source_auth=arc_auth_header_source,
        target_auth=arc_auth_header_target,
        source_website=args.source_website,
        target_website=args.target_website,
        video_urlstring=args.video_urlstring,
        gallery_urlstring=args.gallery_urlstring,
        dry_run=args.dry_run,
        test_run=args.test_run,
    ).doit()

