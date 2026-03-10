from typing import Any, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


# Base class for JSON-LD compatibility
class SchemaDotOrgBase(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={"@context": "https://schema.org"},
        populate_by_name=True,
        extra="allow",  # schema.org allows unknown properties
    )


class PostalAddress(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: str = Field(..., description="json-ld id")
    streetAddress: Optional[str] = Field(None, description="Street address")
    addressLocality: Optional[str] = None
    addressRegion: Optional[str] = None
    postalCode: Optional[str] = None
    addressCountry: Optional[str] = None


class Person(SchemaDotOrgBase):
    """schema.org/Person - Human being."""

    type: Literal["Person"] = Field("Person", frozen=True)
    id: str = Field(..., description="json-ld id")
    name: str = Field(..., description="Full name")
    givenName: Optional[str] = Field(None, description="First/given name")
    familyName: Optional[str] = Field(None, description="Last/family name")
    email: Optional[str] = Field(None, description="Email address")
    telephone: Optional[str] = Field(None, description="Phone number")
    url: Optional[str] = Field(None, description="Website/URL")
    image: Optional[Union[str, List[str]]] = Field(None, description="Image URL(s)")
    jobTitle: Optional[str] = Field(None, description="Job title")
    worksFor: Optional[Union["Organization", str]] = Field(None, description="Employer")
    address: Optional[Union[PostalAddress, str]] = Field(
        None, description="Postal address"
    )
    knows: Optional[List["Person"]] = Field(
        default_factory=list, description="Known people"
    )


class Dataset(SchemaDotOrgBase):
    """schema.org/Dataset - Collection of data."""

    type: Literal["Dataset"] = Field("Dataset", frozen=True)
    id: str = Field(..., description="json-ld id")
    name: str = Field(..., description="Dataset name/title")
    description: Optional[str] = Field(None, description="Description")
    url: Optional[str] = Field(None, description="Dataset URL")
    license: Optional[str] = Field(None, description="License")
    creator: Optional[
        Union["Person", "Organization", List[Union["Person", "Organization"]]]
    ] = Field(None, description="Creator(s)")
    publisher: Optional[Union["Organization", str]] = Field(
        None, description="Publisher"
    )
    datePublished: Optional[str] = Field(None, description="Publication date")
    dateModified: Optional[str] = Field(None, description="Last modification date")
    keywords: Optional[List[str]] = Field(default_factory=list, description="Keywords")
    distribution: Optional[List[Any]] = Field(
        default_factory=list, description="Distributions"
    )
    temporalCoverage: Optional[str] = Field(None, description="Temporal coverage")


class Organization(SchemaDotOrgBase):
    """schema.org/Organization - Business or group."""

    type: Literal["Organization"] = Field("Organization", frozen=True)
    name: str = Field(..., description="Organization name")
    id: str = Field(..., description="json-ld id")
    description: Optional[str] = Field(None, description="Description")
    url: Optional[str] = Field(None, description="Website")
    logo: Optional[Union[str, List[str]]] = Field(None, description="Logo image")
    address: Optional[Union[PostalAddress, str]] = Field(None, description="Address")
    contactPoint: Optional[Any] = Field(None, description="Contact info")
    email: Optional[str] = Field(None, description="Email")
    telephone: Optional[str] = Field(None, description="Phone")
    legalName: Optional[str] = Field(None, description="Legal name")
    foundingDate: Optional[str] = Field(None, description="Founding date")
