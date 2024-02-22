import argparse
import pprint
from dataclasses import dataclass
from typing import Optional

import arc2arc_exceptions
import arc_endpoints
import arc_id
import dist_ref_id
import jmespath
import json
import requests


@dataclass
class MigrationJson:
    ANS: dict
    arcAdditionalProperties: dict


@dataclass
class DocumentReferences:
    images: Optional[dict] = None
    distributor: Optional[dict] = None
    authors: Optional[list] = None
    circulation: Optional[dict] = None


class Arc2ArcGallery:
    """
    Usage: Copy one Gallery via its arc id from source organization into target organization (production environments).
    - The script sets up a class where an ETL process takes place
    - CLass properties are modified by class methods, resulting in the transformed ANS.
    - There is a class method to extract an object's data from Arc, several class methods to apply other transformations,
    a class method to validate the transformed ANS, and a class method to load transformed ANS into a target organization.
    - Start by looking at the doit() method at the bottom of the script.

    Results:
    - Gallery will exist in target organization's production environment.
    - References used in this gallery will be cataloged and a list of them returned.
    - The objects behind the references will not be moved into the target organization.
    - Image references are reconstructed to use new image ids.
    This re-id is necessary when moving Photo Center objects to a new org.
    It is not possible to maintain the same image or gallery ids between different orgs.
    - Script will circulate the gallery to one replacement website + section in target org,
    or circulate to the target website but the exact same named section, based on parameters passed to script
    - see --to-website-section at bottom of script
    - Distributor property in the ANS will be written to use the target org distributor ids if they have been created in the target org.
    - Script will attempt to create Distributors used in ANS in the target org.
    - Does not cause the objects in the references to be ingested to the target organization, other than distributors.
    - The returned catalog of references self.references can be used to inform additional operations
    that might be necessary to bring the referenced objects into the target organization.

    Example terminal usage:
    python this_script.py --from-org devtraining --to-org cetest --gallery-arc_id MBDJUMH35VA4VKRW2Y6S2IR44A --from-token devtraining prod token --to-token cetest prod token --to-website-site cetest --to-website-section /test  --dry-run 1

    :modifies:
        self.references: {}
        self.ans: {}
        self.message: ""

    """
    def __init__(
        self,
        arc_id,
        from_org,
        to_org,
        source_auth,
        target_auth,
        target_website,
        target_section,
        dry_run,
    ):
        self.dry_run = bool(int(dry_run))
        self.arc_auth_header_source = source_auth
        self.arc_auth_header_target = target_auth
        self.from_org = from_org
        self.to_org = to_org
        self.gallery_arc_id = arc_id
        self.regen_gallery_arc_id = ""
        self.target_website = target_website
        self.target_section = target_section
        self.ans = {}
        self.references = DocumentReferences()
        self.message = ""
        self.validation = None
        self.dry_run_restriction_msg = "new distributors are not created during a dry run"

    def fetch_source_ans(self):
        """
        Pulls back the source ANS with the given gallery ANS id.
        Checks the exact ANS id from the source org, then checks the regenerated id as it would be in the target org.
        if the Gallery already exists, resulting ANS is not returned.

        :modifies:
            self.ans
            self.message
        """
        if self.dry_run:
            print(
                "THIS IS A TEST RUN. NEW GALLERY WILL NOT BE CREATED. NEW DISTRIBUTORS AND RESTRICTIONS WILL NOT BE CREATED."
            )

        # has this gallery already been created in the target org?  check the exact arc id
        gallery_exists_res = requests.get(
            arc_endpoints.get_galleries_url(self.to_org, self.gallery_arc_id),
            headers=self.arc_auth_header_target,
        )
        if gallery_exists_res.ok and gallery_exists_res.json():
            # testing for user error running this script, if passed a target org arc id, not a source org arc id
            self.message = (
                f"Gallery exists in {self.to_org} w/ id passed to script {self.gallery_arc_id}, "
                f"{gallery_exists_res}, {gallery_exists_res.json()}"
            )

        else:
            # now check the target arc id that would be created if this id came from the source organization
            self.regen_gallery_arc_id = arc_id.generate_arc_id(
                self.gallery_arc_id, self.to_org
            )
            gallery_exists_res = requests.get(
                arc_endpoints.get_galleries_url(self.to_org, self.regen_gallery_arc_id),
                headers=self.arc_auth_header_target,
            )
            if gallery_exists_res.ok and gallery_exists_res.json():
                self.message = (
                    f"Gallery exists w/ id {self.gallery_arc_id} from {self.from_org} org regenerated "
                    f"to the id {self.regen_gallery_arc_id} for {self.to_org}, {gallery_exists_res}, {gallery_exists_res.json()}"
                )

            else:
                # Retrieve source ANS
                gallery_res = requests.get(
                    arc_endpoints.get_galleries_url(self.from_org, self.gallery_arc_id),
                    headers=arc_auth_header_source,
                )
                if gallery_res.ok:
                    self.ans = gallery_res.json()
                else:
                    self.message = f"{gallery_res} {self.from_org} {self.gallery_arc_id} {gallery_res.text}"

    def transform_ans(self):
        """
        removes properties necessary to allow object to be ingested into new org
        sets properties with values appropriate to target org
        sets version to specific ANS version
        sets an additional_properties value to track that the object in the target org originated from the source org

        :modifies:
            self.ans
        """
        self.ans.get("owner", {}).update({"id": self.to_org})
        self.ans["_id"] = self.regen_gallery_arc_id
        self.ans["version"] = "0.10.9"
        self.ans["additional_properties"].pop("version", None)
        self.ans["additional_properties"][
            "ingestionMethod"
        ] = f"moved orgs from {self.from_org} to {self.to_org}"
        self.ans["additional_properties"]["arcOriginalId"] = {
            "org": self.from_org,
            "_id": self.gallery_arc_id,
        }

    def other_supporting_references(self):
        """
        related content on a gallery is supported in the ANS but not in the Photo Center UI, so is not represented in this script
            - 04_transform_video_to_sandbox.py version of this method shows transforming related_content
        script does not create redirects
            - redirects attached to the Gallery are possible, but they are not represented in the Gallery ANS directly
            - it is not possible to discover the gallery redirects using a gallery's arc id or gallery canonical url
            - to find gallery redirects you must query content api `type: redirect` and then run a 2nd query using the
                url returned from 1st query to determine if it is for a video
            - see 11_tranform_redirects_all.py

        :modifies:
            self.references
        """

        # credits.by saved in guest/local format won't pass validation if version is included and is mismatch with top-level ANS version
        authors = jmespath.search("credits.by[*].name", self.ans)
        if authors:
            for index, c in enumerate(self.ans["credits"]["by"]):
                try:
                    self.ans["credits"]["by"][index].pop("version", None)
                except:
                    pass

        references_authors = jmespath.search("credits.by[*].referent.id", self.ans)
        if references_authors:
            self.references.authors = references_authors

    def transform_circulation(self):
        """
        Galleries don't retain reference syntax when fetched from the API, but the reference syntax is necessary to ingest a new gallery object.

        reformat `taxonomy.primary_section`, `taxonomy.section` property to use references
        reformat `websites` property

        if target section is passed into script ( see --to-website-section at bottom of script), will circulate to only that section in target website
        otherwise, will circulate to all the original sections that were in the source circulation, but to only the one website
        does not create sections in the target org, only writes references in the ANS
        multiple website or multiple section logic and behavior programming is not in the scope of this script
        the value then in canonical url and websites.{site-id}.website_url will be the same.

        :modifies:
            self.references

        - in this script self.references.circulation contains information-only data
        - there are no seperate Gallery circulation objects that needs to be ingested, as with a story
        - information is provided as a way to validate the source website/section data vs the transformed target website/section data
        """
        orig_section = jmespath.search(
            "taxonomy.primary_site._id || taxonomy.primary_site.referent.id", self.ans
        )
        # add original circulation info to the references structure
        source_circulation = jmespath.search(
            "taxonomy.sections[*].{section: _id || referent.id, website: _website || referent.website}[]",
            self.ans,
            jmespath.Options(dict_cls=dict),
        )
        self.references.circulation = {self.from_org: source_circulation}

        if self.target_section:
            section_reference = {
                "type": "reference",
                "referent": {
                    "id": self.target_section,
                    "type": "section",
                    "website": self.target_website,
                },
            }
            self.ans["taxonomy"]["primary_section"] = section_reference
            self.ans["taxonomy"]["sections"] = [section_reference]
        else:
            section_reference = {
                "type": "reference",
                "referent": {
                    "id": orig_section,
                    "type": "section",
                    "website": self.target_website,
                },
            }
            self.ans["taxonomy"]["primary_section"] = section_reference
            for index, s in enumerate(self.ans["taxonomy"].get("sections")):
                section_id = jmespath.search("_id || referent.id", s)
                section_reference = {
                    "type": "reference",
                    "referent": {
                        "id": section_id,
                        "type": "section",
                        "website": self.target_website,
                    },
                }
                self.ans["taxonomy"]["sections"][index] = section_reference

        # add updated circulation to the references structure
        target_circulation = jmespath.search(
            "[*].{section: referent.id, website: referent.website}[]",
            self.ans["taxonomy"]["sections"],
            jmespath.Options(dict_cls=dict),
        )
        self.references.circulation = {self.to_org: target_circulation}

        # websites : assumes the gallery is published to just one website, and there is just the one url.
        # the value then in canonical url and websites.{site-id}.website_url will be the same.
        self.ans["canonical_website"] = self.target_website
        self.ans["websites"] = {
            self.target_website: {
                "website_url": self.ans.get("canonical_url", ""),
                "website_section": section_reference,
            }
        }
        self.ans.pop("canonical_url", None)
        self.ans["taxonomy"].pop("primary_site", None)
        self.ans["taxonomy"].pop("sites", None)

    def transform_content_elements(self):
        """
        Galleries don't retain reference syntax when fetched from the API, but the reference syntax is necessary to ingest a new gallery object.

        Finds and rewrites Photo Center references in ANS that will need to be ingested into target organization.
        Re-ids references from Photo Center objects and documents both old and new ids in the return object self.references
        New ids are used in the rewritten references.

        :modifies:
            self.references
            self.ans
        """
        # make the image ids in the content_elements unique. hash(original_id + org_id)
        ce_imgs = self.ans["content_elements"]
        references_images_newids = {}
        for index, element in enumerate(ce_imgs):
            old_id = element["_id"]
            # generate new arc id for this photo center object
            regen_id = arc_id.generate_arc_id(old_id, self.to_org)
            # build the display information for self.references
            references_images_newids.update({old_id: regen_id})
            # rewrite the ANS reference in content_elements
            element = {
                "type": "reference",
                "_id": regen_id,
                "referent": {
                    "id": regen_id,
                    "type": "image",
                    "referent_properties": {
                        "additional_properties": {"original_arc_id": old_id}
                    },
                },
            }
            ce_imgs[index] = element
        # update the display information in self.references
        self.references.images = {self.from_org: self.to_org}
        self.references.images.update(references_images_newids)

    def transform_distributor(self):
        """
        Figure out what the new distributor id for target org should be, update in ANS
        If no distributor already exists in target org, script will attempt to create distributor and its restrictions
        If create of target distributor does not work, the distributor.reference_id in story ans will be set to None
        and story ANS will fail validation
        If ANS fails validation because of a None distributor, create the target distributor first with same details as source,
        and come back to this script and transform ANS

        :modifies:
            self.references
            self.ans
        """
        if not self.dry_run:
            (
                self.ans,
                references_distributor,
            ) = dist_ref_id.create_target_distributor_restrictions(
                self.from_org,
                self.to_org,
                self.ans,
                self.arc_auth_header_source,
                self.arc_auth_header_target,
                self.target_website,
            )
            self.references.distributor = references_distributor
            self.references.distributor.update(
                {self.from_org: self.to_org}
            ) if references_distributor else None

        if jmespath.search("distributor.reference_id", self.ans):
            orig_dist_id = self.ans["distributor"]["reference_id"]
            if self.dry_run:
                self.ans["distributor"]["reference_id"] = self.dry_run_restriction_msg
                self.references.distributor = {
                    self.from_org: self.to_org,
                    orig_dist_id: self.dry_run_restriction_msg,
                }
            else:
                self.ans["distributor"]["reference_id"] = references_distributor.get(
                    orig_dist_id, None
                )

    def transform_promo_item(self):
        """
        Galleries don't retain reference syntax when fetched from the API, but the reference syntax is necessary to ingest a new gallery object.

        Finds and rewrites Photo Center references in ANS that will need to be ingested into target organization.
        Re-ids references from Photo Center objects.  New ids are used in the rewritten references.

        :modifies:
            self.ans
        """
        # not adding this id to self.references because it must be one of the ids already found in content_elements
        if self.ans.get("promo_items", {}).get("basic"):
            old_id = self.ans["promo_items"]["basic"]["_id"]
            regen_id = arc_id.generate_arc_id(old_id, self.to_org)
            self.ans["promo_items"]["basic"] = {
                "_id": regen_id,
                "type": "reference",
                "referent": {
                    "type": "image",
                    "id": regen_id,
                    "referent_properties": {
                        "additional_properties": {"original_arc_id": f"{old_id}"}
                    },
                },
            }

    def validate_transform(self):
        # Validate transformed ANS
        try:
            gallery_res2 = requests.post(
                arc_endpoints.ans_validation_url(self.to_org),
                headers=self.arc_auth_header_target,
                json=self.ans,
            )
            if gallery_res2.ok:
                self.validation = True
            else:
                self.message = f"{gallery_res2} {gallery_res2.text}"
                self.validation = False

            # raise custom error only if the error is due to creating a new distributor. should only happen the first time a new distributor is attempted.
            if gallery_res2.status_code == 400 and jmespath.search("[*].message", json.loads(gallery_res2.text)) == ['should NOT have additional properties', 'should be equal to one of values', 'should be string', 'should match exactly one schema in oneOf']:
                raise arc2arc_exceptions.MakingNewDistributorFirstTimeException

        except Exception as e:
            self.message = f"{str(e)} full error: {gallery_res2.text}" if e.__module__ == "arc2arc_exceptions" else f"{gallery_res2} {gallery_res2.text}"
        else:
            print("gallery validation", self.validation, self.gallery_arc_id)

    def post_transformed_ans(self):
        # post transformed ans to new organization
        mc = MigrationJson(self.ans, {})
        self.message = None
        try:
            gallery_res3 = requests.post(
                arc_endpoints.mc_create_ans_url(self.to_org),
                headers=self.arc_auth_header_target,
                json=mc.__dict__,
                params={"ansId": self.gallery_arc_id, "ansType": "gallery"},
            )
            if not gallery_res3.ok:
                raise arc2arc_exceptions.ArcObjectToMigrationCenterFailed

        except Exception as e:
            self.message = f"{str(e)} {gallery_res3.status_code} {gallery_res3.reason} {gallery_res3.text}"
        else:
            print(f"ans posted to {self.to_org} Migration Center", gallery_res3)

    def doit(self):
        self.fetch_source_ans()
        if not self.ans:
            return self.message, None
        self.transform_ans()
        self.other_supporting_references()
        self.transform_content_elements()
        self.transform_promo_item()
        self.transform_distributor()
        self.transform_circulation()
        self.validate_transform()
        if not self.validation:
            return self.message, None
        elif not self.dry_run:
            self.post_transformed_ans()
            if self.message:
                print(self.message)
        return {"references": self.references.__dict__, "ans": self.ans}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--from-org",
        dest="org",
        help="source organization id value; org for production or sandbox.org for sandbox",
        required=True,
        default="",
    )
    parser.add_argument(
        "--to-org",
        dest="to_org",
        help="target organization id value; org for production or sandbox.org for sandbox",
        required=True,
        default="",
    )
    parser.add_argument(
        "--from-token",
        dest="from_token",
        help="source organization bearer token; production environment",
        required=True,
        default="",
    )
    parser.add_argument(
        "--to-token",
        dest="to_token",
        help="target organization bearer token; production environment",
        required=True,
        default="",
    )
    parser.add_argument(
        "--to-website-site",
        dest="to_website",
        help="target organization's website name'",
        required=True,
        default="",
    )
    parser.add_argument(
        "--to-website-section",
        dest="to_section",
        help="target organization's website section  id value.  If none, original sections are retained.",
        required=False,
        default="",
    )
    parser.add_argument(
        "--gallery-arc-id",
        dest="gallery_arc_id",
        help="arc id value of gallery to migrate into target org",
        required=True,
        default="",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        help="Set this to 1 to test the results of transforming an object. The object will not actually post to the target org.",
        required=False,
        default=0,
    )

    args = parser.parse_args()

    arc_auth_header_source = {"Authorization": f"Bearer {args.from_token}"}
    arc_auth_header_target = {"Authorization": f"Bearer {args.to_token}"}

    result = Arc2ArcGallery(
        arc_id=args.gallery_arc_id,
        from_org=args.org,
        to_org=args.to_org,
        source_auth=arc_auth_header_source,
        target_auth=arc_auth_header_target,
        target_website=args.to_website,
        target_section=args.to_section,
        dry_run=args.dry_run,
    ).doit()
    print('\nRESULTS')
    pprint.pp(result)
